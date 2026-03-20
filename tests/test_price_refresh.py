from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from src.fund_tracker.config import EmailConfig, NotificationConfig, TrackerConfig
from src.fund_tracker.database import connect_database
from src.fund_tracker.pricing import PricePoint, _parse_history
from src.fund_tracker.service import FundTrackerService


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


class FakePriceProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_payload(self, fund_code: str):
        self.calls.append(fund_code)
        return type(
            "Payload",
            (),
            {
                "fund_code": fund_code,
                "fund_name": "测试基金",
                "source_name": "fake",
                "latest": PricePoint(
                    price_date=date(2026, 3, 10),
                    nav=1.2345,
                    pct_change_vs_prev=0.8,
                ),
            },
        )()


class FakePriceProviderWithHistory:
    def fetch_payload(self, fund_code: str):
        return type(
            "Payload",
            (),
            {
                "fund_code": fund_code,
                "fund_name": "测试基金",
                "source_name": "fake",
                "history": [
                    PricePoint(price_date=date(2026, 3, 9), nav=1.2, pct_change_vs_prev=0.5),
                    PricePoint(price_date=date(2026, 3, 10), nav=1.25, pct_change_vs_prev=0.8),
                ],
                "latest": PricePoint(price_date=date(2026, 3, 10), nav=1.25, pct_change_vs_prev=0.8),
            },
        )()


class PriceRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.config = make_config(self.tempdir.name)
        self.conn = connect_database(self.config.db_path)
        self.provider = FakePriceProvider()
        self.service = FundTrackerService(self.conn, self.config, price_provider=self.provider)

    def tearDown(self) -> None:
        self.conn.close()
        self.tempdir.cleanup()

    def test_parse_history_uses_china_timezone_for_trade_date(self) -> None:
        history = _parse_history(
            [
                {
                    "x": 1773072000000,
                    "y": 1.277,
                    "equityReturn": -0.17,
                }
            ]
        )

        self.assertEqual(history[0].price_date.isoformat(), "2026-03-10")

    def test_build_snapshot_refreshes_stale_prices_for_tracked_funds(self) -> None:
        self.service._upsert_fund("022485", "旧基金名", 1.5)
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-08",
            trade_type="initial",
            amount=1000,
            nav=1.2,
            shares=800,
            fee=0,
            source="initial",
            status="posted",
            note="init",
            raw_text="init",
            plan_id=None,
        )
        self.service._upsert_daily_price(
            "022485",
            PricePoint(
                price_date=date(2026, 3, 8),
                nav=1.2,
                pct_change_vs_prev=0.1,
            ),
            "seed",
        )
        self.service._refresh_auto_fund_limits = lambda: None  # type: ignore[method-assign]
        self.service._build_same_day_execution_context = (  # type: ignore[method-assign]
            lambda execution_date, active_dca_plans: {"tracked_fund_limits": [], "today_due_dca_plans": []}
        )
        self.service._list_active_dca_plans = lambda execution_date: []  # type: ignore[method-assign]

        snapshot = self.service.build_portfolio_snapshot(as_of=date(2026, 3, 10))

        self.assertEqual(self.provider.calls, ["022485"])
        self.assertEqual(snapshot["positions"][0]["latest_price_date"], "2026-03-10")
        self.assertEqual(snapshot["positions"][0]["valuation_as_of_date"], "2026-03-10")
        self.assertEqual(snapshot["positions"][0]["daily_pnl_as_of_date"], "2026-03-10")
        self.assertEqual(snapshot["portfolio"]["valuation_as_of_date_min"], "2026-03-10")
        self.assertEqual(snapshot["portfolio"]["valuation_as_of_date_max"], "2026-03-10")
        self.assertEqual(snapshot["portfolio"]["same_day_priced_position_count"], 1)
        self.assertEqual(snapshot["portfolio"]["one_day_pnl_position_count"], 1)
        row = self.conn.execute(
            """
            SELECT price_date, nav, source_name
            FROM daily_prices
            WHERE fund_code = ?
            ORDER BY price_date DESC
            LIMIT 1
            """,
            ("022485",),
        ).fetchone()
        self.assertEqual(row["price_date"], "2026-03-10")
        self.assertEqual(row["source_name"], "fake")

    def test_settlement_dates_move_weekend_order_to_next_trade_day(self) -> None:
        history = [
            PricePoint(price_date=date(2026, 3, 6), nav=1.1, pct_change_vs_prev=None),
            PricePoint(price_date=date(2026, 3, 9), nav=1.2, pct_change_vs_prev=0.5),
            PricePoint(price_date=date(2026, 3, 10), nav=1.25, pct_change_vs_prev=0.8),
        ]

        settled = self.service._resolve_transaction_settlement_dates(  # type: ignore[attr-defined]
            history=history,
            order_at=datetime(2026, 3, 8, 10, 0, 0),
            fund_code="022485",
            fund_name="测试基金",
        )

        self.assertIsNotNone(settled)
        confirm_point, effective_from_date = settled  # type: ignore[misc]
        self.assertEqual(confirm_point.price_date.isoformat(), "2026-03-09")
        self.assertEqual(effective_from_date, "2026-03-10")

    def test_settlement_rule_can_make_effective_date_wednesday_for_qdii_like_fund(self) -> None:
        history = [
            PricePoint(price_date=date(2026, 3, 9), nav=1.2, pct_change_vs_prev=0.5),
            PricePoint(price_date=date(2026, 3, 10), nav=1.21, pct_change_vs_prev=0.8),
            PricePoint(price_date=date(2026, 3, 11), nav=1.22, pct_change_vs_prev=0.7),
        ]
        self.service._external_research_engine = lambda: type(  # type: ignore[method-assign]
            "FakeEngine",
            (),
            {
                "lookup_fund_settlement_rule": staticmethod(
                    lambda fund_code, fund_name='': {
                        "cutoff_time": "15:00",
                        "confirm_trade_day_lag": 0,
                        "effective_trade_day_lag_after_confirm": 2,
                    }
                )
            },
        )()

        settled = self.service._resolve_transaction_settlement_dates(  # type: ignore[attr-defined]
            history=history,
            order_at=datetime(2026, 3, 8, 10, 0, 0),
            fund_code="161125",
            fund_name="易方达标普500指数人民币A(QDII)",
        )

        self.assertIsNotNone(settled)
        confirm_point, effective_from_date = settled  # type: ignore[misc]
        self.assertEqual(confirm_point.price_date.isoformat(), "2026-03-09")
        self.assertEqual(effective_from_date, "2026-03-11")

    def test_daily_pnl_shares_exclude_buys_not_effective_yet(self) -> None:
        self.service._upsert_fund("022485", "测试基金", 1.5)
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-07",
            trade_type="initial",
            amount=1000,
            nav=1.0,
            shares=1000,
            fee=0,
            source="initial",
            status="posted",
            note="initial",
            raw_text="initial",
            plan_id=None,
            order_date="2026-03-07",
            confirm_nav_date="2026-03-07",
            effective_from_date="2026-03-07",
        )
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-09",
            trade_type="buy",
            amount=1000,
            nav=1.0,
            shares=1000,
            fee=0,
            source="manual",
            status="posted",
            note="weekend order",
            raw_text="buy",
            plan_id=None,
            order_date="2026-03-08",
            confirm_nav_date="2026-03-09",
            effective_from_date="2026-03-10",
        )

        shares_before_0309 = self.service._shares_held_before_price_date(  # type: ignore[attr-defined]
            fund_code="022485",
            price_date="2026-03-09",
            current_shares=2000,
        )
        shares_before_0310 = self.service._shares_held_before_price_date(  # type: ignore[attr-defined]
            fund_code="022485",
            price_date="2026-03-10",
            current_shares=2000,
        )

        self.assertEqual(shares_before_0309, 1000)
        self.assertEqual(shares_before_0310, 2000)

    def test_buy_action_persists_purchase_fee_from_fee_rate(self) -> None:
        self.service.price_provider = FakePriceProviderWithHistory()  # type: ignore[assignment]
        self.service._external_research_engine = lambda: type(  # type: ignore[method-assign]
            "FakeEngine",
            (),
            {"lookup_fund_purchase_fee_rate": staticmethod(lambda fund_code, fund_name='': 0.15)},
        )()

        result = self.service._execute_trade_plan_action(  # type: ignore[attr-defined]
            action={
                "action_type": "buy",
                "fund_code": "022485",
                "fund_name": "测试基金",
                "amount": 1000,
            },
            command_date=date(2026, 3, 8),
            report_id=1,
        )

        self.assertEqual(result["trade_date"], "2026-03-09")
        self.assertEqual(result["effective_from_date"], "2026-03-10")
        self.assertEqual(result["fee"], 1.5)
        row = self.conn.execute(
            """
            SELECT fee, order_date, confirm_nav_date, effective_from_date
            FROM transactions
            WHERE fund_code = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("022485",),
        ).fetchone()
        self.assertEqual(float(row["fee"]), 1.5)
        self.assertEqual(row["order_date"], "2026-03-08")
        self.assertEqual(row["confirm_nav_date"], "2026-03-09")
        self.assertEqual(row["effective_from_date"], "2026-03-10")

    def test_backfill_purchase_fees_updates_historical_buy_and_dca(self) -> None:
        self.service._upsert_fund("022485", "测试基金", 1.5)
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-08",
            trade_type="buy",
            amount=1000,
            nav=1.2,
            shares=833.3333,
            fee=0,
            source="manual",
            status="posted",
            note="buy",
            raw_text="buy",
            plan_id=None,
        )
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-09",
            trade_type="dca",
            amount=500,
            nav=1.25,
            shares=400,
            fee=0,
            source="auto_dca",
            status="posted",
            note="dca",
            raw_text="dca",
            plan_id=None,
        )
        self.service._external_research_engine = lambda: type(  # type: ignore[method-assign]
            "FakeEngine",
            (),
            {"lookup_fund_purchase_fee_rate": staticmethod(lambda fund_code, fund_name='': 0.15)},
        )()

        result = self.service.backfill_purchase_fees()

        self.assertEqual(result.payload["transaction_count"], 2)
        rows = self.conn.execute(
            """
            SELECT trade_type, fee
            FROM transactions
            WHERE fund_code = ?
            ORDER BY id ASC
            """,
            ("022485",),
        ).fetchall()
        self.assertEqual(float(rows[0]["fee"]), 1.5)
        self.assertEqual(float(rows[1]["fee"]), 0.75)

    def test_price_freshness_diagnostics_reports_stale_rows(self) -> None:
        self.service._upsert_fund("022485", "测试基金", 1.5)
        self.service._insert_transaction(
            fund_code="022485",
            trade_date="2026-03-08",
            trade_type="initial",
            amount=1000,
            nav=1.2,
            shares=800,
            fee=0,
            source="initial",
            status="posted",
            note="init",
            raw_text="init",
            plan_id=None,
        )
        self.service._upsert_daily_price(
            "022485",
            PricePoint(
                price_date=date(2026, 3, 8),
                nav=1.2,
                pct_change_vs_prev=0.1,
            ),
            "seed",
        )

        payload = self.service.get_price_freshness_diagnostics(as_of=date(2026, 3, 10))

        self.assertEqual(payload["as_of_date"], "2026-03-10")
        self.assertEqual(payload["total_funds"], 1)
        self.assertEqual(payload["stale_funds"], 1)
        self.assertEqual(payload["items"][0]["lag_days"], 2)


if __name__ == "__main__":
    unittest.main()
