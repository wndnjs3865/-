"""
Quant Trading Bot - Professional Multi-Agent Automated Trading System
=====================================================================
전문 Quant Fund급 Multi-Agent 자동매매 봇

Architecture:
- Multi-Agent System: 6 specialized agents coordinated by orchestrator
- Event-Driven: Async event bus for inter-agent communication
- Risk-First: VaR, position sizing, drawdown protection
- Multi-Strategy: Momentum, Mean Reversion, Multi-Factor

Agents:
- MarketDataAgent: Real-time & historical data collection
- AnalysisAgent: Technical & fundamental analysis
- SentimentAgent: News & market sentiment analysis
- RiskAgent: Risk management & position sizing
- ExecutionAgent: Order execution via KIS API
- PortfolioAgent: Portfolio optimization & rebalancing
"""

__version__ = "1.0.0"
__author__ = "Quant Trading Bot"
