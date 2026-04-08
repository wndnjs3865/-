"""
Data Cache
==========
In-memory LRU cache with TTL for market data.
Redis optional for distributed caching.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional

from loguru import logger


@dataclass
class CacheEntry:
    value: Any
    expires_at: float
    access_count: int = 0


class DataCache:
    """
    In-memory cache with TTL and LRU eviction.

    Features:
    - TTL-based expiration
    - LRU eviction when max size reached
    - Hit/miss statistics
    - Namespace support
    """

    def __init__(self, max_size: int = 10_000, default_ttl: int = 300):
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0
        self._log = logger.bind(agent_name="CACHE")

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key not in self._store:
            self._misses += 1
            return None

        entry = self._store[key]
        if time.time() > entry.expires_at:
            del self._store[key]
            self._misses += 1
            return None

        entry.access_count += 1
        self._hits += 1
        self._store.move_to_end(key)
        return entry.value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with TTL."""
        if len(self._store) >= self._max_size:
            self._store.popitem(last=False)

        self._store[key] = CacheEntry(
            value=value,
            expires_at=time.time() + (ttl or self._default_ttl),
        )

    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        if key in self._store:
            del self._store[key]
            return True
        return False

    def clear(self, namespace: Optional[str] = None) -> int:
        """Clear cache, optionally by namespace prefix."""
        if namespace is None:
            count = len(self._store)
            self._store.clear()
            return count

        keys_to_delete = [k for k in self._store if k.startswith(namespace)]
        for k in keys_to_delete:
            del self._store[k]
        return len(keys_to_delete)

    def cleanup_expired(self) -> int:
        """Remove all expired entries."""
        now = time.time()
        expired = [k for k, v in self._store.items() if now > v.expires_at]
        for k in expired:
            del self._store[k]
        return len(expired)

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0

    def get_stats(self) -> dict:
        return {
            "size": len(self._store),
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self.hit_rate:.1%}",
        }
