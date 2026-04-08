"""Tests for QuantPulse v3 core models."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.enums import Direction, MarketRegime, Priority, Timeframe, AgentRole
from core.models import (
    Message, TradeSignal, MultiTimeframeAnalysis, MarketStructureReport,
    QuantScore, RiskAssessment, RiskValidation, TradeDecision,
    OrderResult, HealthStatus, AlertEvent, AuditEntry, TimeframeAnalysis,
)


def test_message_creation():
    msg = Message(
        topic="test.topic",
        sender=AgentRole.MIA,
        priority=Priority.HIGH,
        payload={"key": "value"},
    )
    assert msg.topic == "test.topic"
    assert msg.sender == AgentRole.MIA
    assert msg.priority == Priority.HIGH
    assert msg.payload["key"] == "value"
    assert len(msg.id) == 16


def test_trade_signal():
    signal = TradeSignal(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        score=85.0,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=52000.0,
        take_profit_2=54000.0,
        take_profit_3=56000.0,
        timeframe=Timeframe.H4,
        regime=MarketRegime.TRENDING_BULL,
        risk_reward_ratio=3.0,
        confluence_factors=["BOS", "OB", "FVG"],
    )
    assert signal.symbol == "BTCUSDT"
    assert signal.direction == Direction.LONG
    assert signal.risk_reward_ratio == 3.0
    assert len(signal.confluence_factors) == 3


def test_market_structure_report():
    report = MarketStructureReport(
        symbol="ETHUSDT",
        timeframe=Timeframe.H1,
        regime=MarketRegime.TRENDING_BULL,
        trend_direction=Direction.LONG,
        trend_strength=0.85,
        break_of_structure=True,
        key_levels=[3000.0, 3100.0, 3200.0],
    )
    assert report.break_of_structure is True
    assert report.trend_strength == 0.85


def test_quant_score_compute():
    qs = QuantScore(
        signal_id="test123",
        symbol="BTCUSDT",
        trend_alignment=90.0,
        momentum_score=85.0,
        volume_profile=80.0,
        volatility_regime=75.0,
        orderflow_imbalance=88.0,
        correlation_filter=70.0,
        statistical_edge=82.0,
        timing_score=78.0,
        total_score=0.0,
        grade="",
    )
    total = qs.compute_total()
    assert 80.0 < total < 90.0  # Weighted average
    grade = qs.compute_grade()
    assert grade in ("A+", "A", "B+")


def test_risk_assessment_veto():
    ra = RiskAssessment(
        signal_id="test123",
        symbol="BTCUSDT",
        approved=False,
        veto_reasons=["Daily loss limit exceeded", "Correlation too high"],
        validations=[
            RiskValidation(check_name="daily_loss", passed=False, value=0.04, threshold=0.03),
            RiskValidation(check_name="correlation", passed=False, value=0.8, threshold=0.7),
        ],
        risk_score=85.0,
    )
    assert ra.vetoed is True
    assert len(ra.veto_reasons) == 2
    assert ra.validations[0].passed is False


def test_risk_assessment_approved():
    ra = RiskAssessment(
        signal_id="test456",
        symbol="ETHUSDT",
        approved=True,
        risk_score=30.0,
        max_position_size=0.1,
        recommended_leverage=3.0,
    )
    assert ra.vetoed is False
    assert ra.approved is True


def test_trade_decision():
    td = TradeDecision(
        signal_id="sig001",
        symbol="BTCUSDT",
        direction=Direction.LONG,
        approved=True,
        entry_price=50000.0,
        stop_loss=49000.0,
        take_profit_1=52000.0,
        position_size=0.1,
        leverage=3.0,
        quant_score=82.5,
        quant_grade="A",
        risk_score=25.0,
    )
    assert td.approved is True
    assert td.leverage == 3.0


def test_multi_timeframe_analysis():
    analyses = [
        TimeframeAnalysis(
            timeframe=Timeframe.D1,
            structure=MarketStructureReport(
                symbol="BTCUSDT",
                timeframe=Timeframe.D1,
                regime=MarketRegime.TRENDING_BULL,
                trend_direction=Direction.LONG,
                trend_strength=0.9,
            ),
            bias=Direction.LONG,
            confidence=0.9,
        ),
        TimeframeAnalysis(
            timeframe=Timeframe.H4,
            structure=MarketStructureReport(
                symbol="BTCUSDT",
                timeframe=Timeframe.H4,
                regime=MarketRegime.TRENDING_BULL,
                trend_direction=Direction.LONG,
                trend_strength=0.85,
            ),
            bias=Direction.LONG,
            confidence=0.85,
        ),
    ]
    mtf = MultiTimeframeAnalysis(
        symbol="BTCUSDT",
        analyses=analyses,
        htf_bias=Direction.LONG,
        ltf_confirmation=True,
        confluence_score=0.88,
        regime=MarketRegime.TRENDING_BULL,
    )
    assert len(mtf.analyses) == 2
    assert mtf.ltf_confirmation is True
    assert mtf.confluence_score == 0.88


def test_audit_entry():
    entry = AuditEntry(
        agent=AgentRole.CRCO,
        action="risk_assessment",
        detail={"signal_id": "test123", "approved": False},
    )
    assert entry.agent == AgentRole.CRCO
    assert entry.action == "risk_assessment"


def test_health_status():
    hs = HealthStatus(
        agent=AgentRole.MIA,
        alive=True,
        uptime_seconds=3600,
        messages_processed=150,
        errors_count=2,
    )
    assert hs.alive is True
    assert hs.messages_processed == 150
