from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.fund_tracker.config import EmailConfig, NotificationConfig, TrackerConfig
from src.fund_tracker.database import connect_database
from src.fund_tracker.service import CommandResult, FundTrackerService


def make_config(tmpdir: str) -> TrackerConfig:
    root = Path(tmpdir)
    config_path = root / "config" / "fund_tracker.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    snapshot_dir = root / "data" / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    return TrackerConfig(
        config_path=config_path,
        db_path=root / "data" / "fund_tracker.db",
        snapshot_dir=snapshot_dir,
        default_drop_threshold_pct=1.5,
        daily_run_time="21:30",
        price_provider="eastmoney_pingzhongdata",
        email=EmailConfig(
            enabled=False,
            smtp_host="",
            smtp_port=465,
            use_ssl=True,
            username="",
            password="",
            sender="",
            recipient="",
        ),
        notifications=NotificationConfig(
            macos_enabled=False,
            title_prefix="Fund Tracker",
        ),
    )


def sample_snapshot() -> dict:
    return {
        "generated_at": "2026-03-09T21:00:00",
        "portfolio": {
            "position_count": 1,
            "total_market_value": 10000.0,
            "total_cost_basis": 9800.0,
            "total_daily_pnl": 0.0,
            "total_unrealized_pnl": 200.0,
            "total_return_pct": 2.04,
            "total_realized_pnl": 0.0,
            "total_return": 200.0,
            "total_net_invested": 9800.0,
            "valuation_as_of_date_min": "2026-03-09",
            "valuation_as_of_date_max": "2026-03-09",
            "valuation_date_count": 1,
            "priced_position_count": 1,
            "same_day_priced_position_count": 1,
            "one_day_pnl_position_count": 1,
            "as_of_target_date": "2026-03-09",
            "valuation_mode": "per_fund_latest_nav",
            "daily_pnl_mode": "per_fund_latest_one_day_move",
        },
        "positions": [],
        "active_dca_plans": [],
        "same_day_execution_context": {
            "tracked_fund_limits": [],
        },
    }


class FakeDailyOpportunityEngine:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def build_daily_opportunity_material_packet(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "report_date": "2026-03-09",
            "available_cash": kwargs.get("available_cash"),
            "portfolio_snapshot": kwargs["snapshot"],
            "priority_industry_watchlist": ["半导体", "消费"],
            "priority_industry_watch_snapshot": [
                {
                    "theme": "半导体",
                    "signal": "positive",
                    "today_summary": "板块偏强。",
                    "representative_funds": [
                        {"fund_code": "020109", "fund_name": "半导体芯片ETF联接A"},
                    ],
                }
            ],
        }


class FakeDailyOpportunityRunner:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls: list[dict] = []

    def generate_daily_opportunity_report(self, material_packet: dict) -> dict:
        self.calls.append(material_packet)
        return dict(self.payload)


class DailyOpportunityServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.config = make_config(self.tempdir.name)
        self.conn = connect_database(self.config.db_path)
        self.service = FundTrackerService(self.conn, self.config)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_generate_daily_opportunity_report_saves_structured_payload(self) -> None:
        engine = FakeDailyOpportunityEngine()
        runner = FakeDailyOpportunityRunner(
            {
                "report_body": "今日结论\n今天有一笔可执行的例外买点。",
                "recommendation_level": "strong_buy",
                "should_alert": True,
                "summary": "国内核心宽基更适合承担今天的新增资金。",
                "no_action_reason": None,
                "opportunities": [
                    {
                        "fund_code": "022485",
                        "fund_name": "国金中证A500指数增强A",
                        "action_type": "buy",
                        "suggested_amount": 1000,
                        "thesis": "补核心仓。",
                        "why_now": "今天更适合切入。",
                        "portfolio_fit": "降低海外单一主线依赖。",
                        "constraint_check": {
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 5000,
                            "today_due_dca_amount": 0,
                            "today_remaining_purchase_capacity": 5000,
                            "same_day_executable": True,
                        },
                        "risks": ["短期仍可能波动。"],
                    }
                ],
                "expires_at": "2026-03-09T23:59:59+08:00",
            }
        )
        notified: list[dict] = []

        self.service._external_research_engine = lambda: engine  # type: ignore[method-assign]
        self.service._codex_monthly_briefing_runner = lambda: runner  # type: ignore[method-assign]
        self.service._send_daily_opportunity_notifications = (  # type: ignore[method-assign]
            lambda report_date, summary, opportunities: notified.append(
                {
                    "report_date": report_date.isoformat(),
                    "summary": summary,
                    "count": len(opportunities),
                }
            )
            or [
                {
                    "fund_code": "022485",
                    "fund_name": "国金中证A500指数增强A",
                    "delivery_status": "skipped",
                    "alert_date": report_date.isoformat(),
                }
            ]
        )

        result = self.service.generate_daily_opportunity_report(
            report_date=date(2026, 3, 9),
            snapshot=sample_snapshot(),
            available_cash=2000,
            notify=True,
        )

        self.assertEqual(result.payload["report_type"], "external_daily_opportunity")
        self.assertEqual(result.payload["recommendation_level"], "strong_buy")
        self.assertTrue(result.payload["should_alert"])
        self.assertEqual(len(result.payload["opportunities"]), 1)
        self.assertEqual(notified[0]["report_date"], "2026-03-09")
        self.assertEqual(engine.calls[0]["available_cash"], 2000)
        self.assertEqual(runner.calls[0]["report_date"], "2026-03-09")

        row = self.conn.execute(
            """
            SELECT report_date, report_type, input_snapshot, skill_name, report_body
            FROM analysis_reports
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["report_date"], "2026-03-09")
        self.assertEqual(row["report_type"], "external_daily_opportunity")
        self.assertIn("fund-daily-opportunity-monitor", row["skill_name"])
        self.assertIn("例外买点", row["report_body"])
        input_snapshot = json.loads(row["input_snapshot"])
        self.assertEqual(input_snapshot["recommendation_level"], "strong_buy")
        self.assertEqual(input_snapshot["available_cash"], 2000.0)
        self.assertEqual(input_snapshot["priority_industry_watchlist"], ["半导体", "消费"])
        self.assertEqual(
            input_snapshot["priority_industry_watch_snapshot"][0]["theme"],
            "半导体",
        )
        self.assertEqual(len(input_snapshot["notification_results"]), 1)

    def test_run_daily_includes_daily_opportunity_summary(self) -> None:
        self.service._materialize_dca_transactions = lambda target_date: [target_date.isoformat()]  # type: ignore[method-assign]
        self.service.refresh_prices = lambda: []  # type: ignore[method-assign]
        self.service._check_alerts = lambda: []  # type: ignore[method-assign]
        self.service.build_portfolio_snapshot = lambda as_of=None: sample_snapshot()  # type: ignore[method-assign]
        self.service.save_snapshot = lambda snapshot: self.config.snapshot_dir / "latest-snapshot.json"  # type: ignore[method-assign]
        self.service._is_month_end = lambda value: False  # type: ignore[method-assign]
        self.service.generate_daily_opportunity_report = (  # type: ignore[method-assign]
            lambda report_date=None, snapshot=None, notify=False, available_cash=None: CommandResult(
                message="已生成今日强机会监测结果。",
                payload={
                    "recommendation_level": "watch",
                    "should_alert": False,
                },
            )
        )

        result = self.service.run_daily(run_date=date(2026, 3, 9))

        self.assertEqual(result.payload["run_date"], "2026-03-09")
        self.assertEqual(len(result.payload["external_reports"]), 1)
        self.assertEqual(
            result.payload["external_reports"][0]["report_type"],
            "external_daily_opportunity",
        )
        self.assertFalse(result.payload["external_reports"][0]["should_alert"])

    def test_daily_opportunity_notifications_respect_cooldown(self) -> None:
        self.service._upsert_fund("022485", "国金中证A500指数增强A", 1.5)
        self.service._insert_alert(
            fund_code="022485",
            alert_date="2026-03-08",
            alert_type="daily_opportunity",
            trigger_value=1.0,
            delivery_status="macos:sent",
            message="昨日已经提醒过。",
        )

        results = self.service._send_daily_opportunity_notifications(
            report_date=date(2026, 3, 9),
            summary="今天仍然强，但不应重复提醒。",
            opportunities=[
                {
                    "fund_code": "022485",
                    "fund_name": "国金中证A500指数增强A",
                    "suggested_amount": 1000,
                }
            ],
        )

        self.assertEqual(
            results,
            [
                {
                    "fund_code": "022485",
                    "fund_name": "国金中证A500指数增强A",
                    "delivery_status": "cooldown:3d",
                    "alert_date": "2026-03-09",
                }
            ],
        )
        count = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM alerts
            WHERE fund_code = '022485' AND alert_type = 'daily_opportunity'
            """
        ).fetchone()["count"]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
