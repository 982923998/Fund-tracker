from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.fund_tracker.external_research import (
    MARKET_SCAN_CATEGORY,
    PRIORITY_INDUSTRY_THEME_SPECS,
    ExternalResearchEngine,
)


def make_engine() -> ExternalResearchEngine:
    tempdir = tempfile.TemporaryDirectory()
    config_path = Path(tempdir.name) / "config.yaml"
    config_path.write_text("sources: []\n", encoding="utf-8")
    cache_dir = Path(tempdir.name) / "cache"
    engine = ExternalResearchEngine(config_path=config_path, cache_dir=cache_dir)
    engine._tempdir = tempdir  # type: ignore[attr-defined]
    return engine


def sample_ranking_rows() -> list[dict]:
    return [
        {"基金代码": "000001", "基金简称": "华安黄金ETF联接A", "近1周": -0.5, "近1月": 3.2, "近1年": 18.6, "手续费": "0.15%"},
        {"基金代码": "000002", "基金简称": "博时黄金ETF联接A", "近1周": -0.3, "近1月": 3.0, "近1年": 17.9, "手续费": "0.15%"},
        {"基金代码": "017436", "基金简称": "华宝纳斯达克精选股票发起式(QDII)A", "近1周": -1.2, "近1月": 2.1, "近1年": 24.0, "手续费": "0.12%"},
        {"基金代码": "021928", "基金简称": "湘财鑫裕纯债A", "近1周": 0.1, "近1月": 0.6, "近1年": 3.5, "手续费": "0.08%"},
        {"基金代码": "022485", "基金简称": "国金中证A500指数增强A", "近1周": -0.9, "近1月": 1.2, "近1年": 12.3, "手续费": "0.12%"},
        {"基金代码": "022486", "基金简称": "中证A500ETF联接A", "近1周": -0.7, "近1月": 1.0, "近1年": 11.1, "手续费": "0.12%"},
        {"基金代码": "022487", "基金简称": "沪深300ETF联接A", "近1周": -0.8, "近1月": 0.8, "近1年": 10.4, "手续费": "0.12%"},
        {"基金代码": "021561", "基金简称": "天弘中证央企红利50指数发起A", "近1周": -0.4, "近1月": 1.8, "近1年": 9.7, "手续费": "0.10%"},
        {"基金代码": "020101", "基金简称": "石油天然气主题混合A", "近1周": 1.3, "近1月": 7.6, "近1年": 20.2, "手续费": "0.15%"},
        {"基金代码": "020102", "基金简称": "有色金属精选混合A", "近1周": 0.9, "近1月": 6.2, "近1年": 18.3, "手续费": "0.15%"},
        {"基金代码": "020103", "基金简称": "中证算力ETF联接A", "近1周": 1.1, "近1月": 5.8, "近1年": 15.4, "手续费": "0.15%"},
        {"基金代码": "020104", "基金简称": "人工智能主题ETF联接A", "近1周": 1.5, "近1月": 6.1, "近1年": 21.9, "手续费": "0.15%"},
        {"基金代码": "020105", "基金简称": "商业航天主题混合A", "近1周": 0.7, "近1月": 4.9, "近1年": 13.4, "手续费": "0.15%"},
        {"基金代码": "020106", "基金简称": "电力设备与储能ETF联接A", "近1周": 0.6, "近1月": 4.1, "近1年": 11.8, "手续费": "0.15%"},
        {"基金代码": "020107", "基金简称": "养老目标2045三年持有混合(FOF)A", "近1周": -0.1, "近1月": 0.9, "近1年": 6.7, "手续费": "0.10%"},
        {"基金代码": "020108", "基金简称": "机器人ETF联接A", "近1周": 1.4, "近1月": 5.4, "近1年": 14.6, "手续费": "0.15%"},
        {"基金代码": "020109", "基金简称": "半导体芯片ETF联接A", "近1周": 1.8, "近1月": 6.5, "近1年": 22.8, "手续费": "0.15%"},
        {"基金代码": "020111", "基金简称": "半导体设备ETF联接A", "近1周": 1.6, "近1月": 6.0, "近1年": 20.1, "手续费": "0.15%"},
        {"基金代码": "020112", "基金简称": "科创芯片ETF联接A", "近1周": 1.7, "近1月": 5.9, "近1年": 19.8, "手续费": "0.15%"},
        {"基金代码": "020113", "基金简称": "半导体材料ETF联接A", "近1周": 1.2, "近1月": 5.1, "近1年": 17.3, "手续费": "0.15%"},
        {"基金代码": "020110", "基金简称": "消费50ETF联接A", "近1周": -0.2, "近1月": 1.4, "近1年": 8.1, "手续费": "0.15%"},
        {"基金代码": "020118", "基金简称": "光伏产业ETF联接A", "近1周": -1.1, "近1月": 2.6, "近1年": 9.8, "手续费": "0.15%"},
        {"基金代码": "020119", "基金简称": "新能源光伏ETF联接A", "近1周": -0.8, "近1月": 2.1, "近1年": 8.7, "手续费": "0.15%"},
        {"基金代码": "020120", "基金简称": "光伏设备主题ETF联接A", "近1周": -0.6, "近1月": 1.9, "近1年": 7.5, "手续费": "0.15%"},
    ]


class CandidateUniverseScopeTests(unittest.TestCase):
    def _make_engine(self) -> ExternalResearchEngine:
        engine = make_engine()
        self.addCleanup(engine._tempdir.cleanup)  # type: ignore[attr-defined]
        return engine

    def test_candidate_universe_is_built_from_all_market_scan_and_priority_themes(self) -> None:
        engine = self._make_engine()
        rows = sample_ranking_rows()
        engine._load_fund_ranking_rows = lambda category, ttl_hours=12: rows if category == MARKET_SCAN_CATEGORY else []  # type: ignore[method-assign]
        engine.lookup_fund_trade_constraint = lambda code, fund_name="": {  # type: ignore[method-assign]
            "purchase_status": "开放申购",
            "daily_purchase_limit_amount": 5000.0,
        }

        context = engine._build_candidate_universe_context(
            positions=[{"fund_code": "000001", "fund_name": "华安黄金ETF联接A"}]
        )

        self.assertEqual(context["candidate_universe_scope"]["source_category"], MARKET_SCAN_CATEGORY)
        self.assertEqual(context["candidate_universe_scope"]["total_funds_scanned"], len(rows))
        self.assertEqual(
            context["priority_industry_watchlist"],
            [item["theme"] for item in PRIORITY_INDUSTRY_THEME_SPECS],
        )

        themes = {item["theme"]: item for item in context["candidate_universe"]}
        for expected_theme in [
            "美股宽基",
            "债券防御",
            "A股宽基",
            "红利低波",
            "石油能源",
            "有色金属",
            "算力基础设施",
            "人工智能",
            "商业航天",
            "电力协同",
            "黄金贵金属",
            "养老",
            "机器人",
            "半导体",
            "消费",
        ]:
            self.assertIn(expected_theme, themes)

        gold_codes = [item["fund_code"] for item in themes["黄金贵金属"]["funds"]]
        self.assertIn("000002", gold_codes)
        self.assertNotIn("000001", gold_codes)
        self.assertEqual(len(themes["A股宽基"]["funds"]), 2)
        self.assertEqual(
            [item["fund_code"] for item in themes["半导体"]["funds"]],
            ["020109", "020111", "020112"],
        )
        self.assertEqual(themes["石油能源"]["funds"][0]["category"], "主动权益")
        self.assertEqual(themes["半导体"]["selection_scope"], "全市场开放式基金扫描")

    def test_daily_material_packet_exposes_scope_and_priority_watchlist(self) -> None:
        engine = self._make_engine()
        engine._summarize_holdings = lambda positions: {"distribution": {}, "top_position": None, "positions": []}  # type: ignore[method-assign]
        engine._build_portfolio_diagnostics = lambda snapshot, holdings_summary: {"facts": []}  # type: ignore[method-assign]
        engine._collect_source_updates = lambda days, per_source: []  # type: ignore[method-assign]
        engine._collect_market_context = lambda: {"indexes": [], "sectors": [], "theme_funds": []}  # type: ignore[method-assign]
        engine._flatten_source_events = lambda source_updates: []  # type: ignore[method-assign]
        engine._build_candidate_universe_context = lambda positions: {  # type: ignore[method-assign]
            "candidate_universe": [{"theme": "半导体", "bucket": "重点行业", "role": "观察芯片。", "reason": "观察芯片。", "funds": []}],
            "candidate_universe_scope": {
                "source_category": MARKET_SCAN_CATEGORY,
                "selection_method": "先全市场扫描，再压缩成重点候选摘要。",
                "total_funds_scanned": 19309,
                "excluded_current_holding_count": 1,
            },
            "priority_industry_watchlist": [item["theme"] for item in PRIORITY_INDUSTRY_THEME_SPECS],
        }
        engine._build_fund_constraints_catalog = lambda snapshot, candidate_universe: []  # type: ignore[method-assign]

        packet = engine.build_daily_opportunity_material_packet(
            snapshot={
                "positions": [{"fund_code": "020109", "fund_name": "半导体芯片ETF联接A"}],
                "same_day_execution_context": {"tracked_fund_limits": []},
            },
            available_cash=2000,
        )

        self.assertEqual(packet["candidate_universe_scope"]["source_category"], MARKET_SCAN_CATEGORY)
        self.assertIn("半导体", packet["priority_industry_watchlist"])
        self.assertEqual(packet["available_cash"], 2000.0)

    def test_describe_priority_industry_watchlist_bootstraps_default_file(self) -> None:
        engine = self._make_engine()

        payload = engine.describe_priority_industry_watchlist()

        self.assertEqual(
            payload["active_themes"],
            [item["theme"] for item in PRIORITY_INDUSTRY_THEME_SPECS],
        )
        self.assertTrue(engine.priority_watchlist_path.exists())

    def test_priority_industry_watch_snapshot_contains_theme_summaries_and_representative_funds(self) -> None:
        engine = self._make_engine()

        snapshot = engine._build_priority_industry_watch_snapshot(
            candidate_universe=[
                {
                    "theme": "半导体",
                    "funds": [
                        {
                            "fund_code": "020109",
                            "fund_name": "半导体芯片ETF联接A",
                            "daily_growth_pct": 1.8,
                            "one_week": 4.2,
                            "one_month": 6.5,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 5000.0,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": 5000.0,
                        },
                        {
                            "fund_code": "020111",
                            "fund_name": "半导体设备ETF联接A",
                            "daily_growth_pct": 1.6,
                            "one_week": 4.0,
                            "one_month": 6.0,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 3000.0,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": 3000.0,
                        },
                        {
                            "fund_code": "020112",
                            "fund_name": "科创芯片ETF联接A",
                            "daily_growth_pct": 1.7,
                            "one_week": 3.8,
                            "one_month": 5.9,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 2000.0,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": 2000.0,
                        },
                    ],
                },
                {
                    "theme": "电力协同",
                    "funds": [
                        {
                            "fund_code": "020106",
                            "fund_name": "电力设备与储能ETF联接A",
                            "daily_growth_pct": 0.6,
                            "one_week": 2.1,
                            "one_month": 4.1,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": None,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": None,
                        },
                        {
                            "fund_code": "020116",
                            "fund_name": "智能电网ETF联接A",
                            "daily_growth_pct": 0.5,
                            "one_week": 1.9,
                            "one_month": 3.7,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 5000.0,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": 5000.0,
                        },
                        {
                            "fund_code": "020117",
                            "fund_name": "储能产业ETF联接A",
                            "daily_growth_pct": 0.8,
                            "one_week": 2.4,
                            "one_month": 4.6,
                            "purchase_status": "开放申购",
                            "daily_purchase_limit_amount": 3000.0,
                            "today_due_dca_amount": 0.0,
                            "today_remaining_purchase_capacity": 3000.0,
                        },
                    ],
                },
            ],
            market_context={
                "sectors": [
                    {"direction": "top", "name": "半导体", "pct_change": 2.3},
                    {"direction": "top", "name": "电网设备", "pct_change": 1.7},
                    {"direction": "bottom", "name": "消费电子", "pct_change": -1.4},
                ],
                "theme_funds": [
                    {"fund_name": "半导体芯片ETF联接A", "fund_code": "020109", "one_month": 6.5},
                ],
            },
            news_events=[
                {"title": "AI 芯片和半导体设备需求继续提升"},
                {"title": "电网设备投资节奏加快，储能协同受关注"},
            ],
        )

        semiconductor = next(item for item in snapshot if item["theme"] == "半导体")
        self.assertEqual(semiconductor["signal"], "positive")
        self.assertIn("板块偏强", semiconductor["today_summary"])
        self.assertEqual(
            [item["fund_code"] for item in semiconductor["representative_funds"]],
            ["020109", "020111", "020112"],
        )

        power_grid = next(item for item in snapshot if item["theme"] == "电力协同")
        self.assertEqual(len(power_grid["representative_funds"]), 3)

        consumer = next(item for item in snapshot if item["theme"] == "消费")
        self.assertEqual(consumer["signal"], "negative")
        self.assertIn("板块承压", consumer["today_summary"])

    def test_priority_industry_watchlist_supports_user_added_theme(self) -> None:
        engine = self._make_engine()
        rows = sample_ranking_rows()
        engine._load_fund_ranking_rows = lambda category, ttl_hours=12: rows if category == MARKET_SCAN_CATEGORY else []  # type: ignore[method-assign]
        engine.lookup_fund_trade_constraint = lambda code, fund_name="": {  # type: ignore[method-assign]
            "purchase_status": "开放申购",
            "daily_purchase_limit_amount": 5000.0,
        }

        watchlist = engine.update_priority_industry_watchlist(["半导体", "光伏", "消费", "半导体"])
        self.assertEqual(watchlist["active_themes"], ["半导体", "光伏", "消费"])
        self.assertEqual(watchlist["custom_themes"], ["光伏"])

        context = engine._build_candidate_universe_context(positions=[])
        priority_themes = [
            item["theme"]
            for item in context["candidate_universe"]
            if item.get("bucket") == "重点行业"
        ]
        self.assertEqual(priority_themes, ["半导体", "光伏", "消费"])
        self.assertEqual(context["priority_industry_watchlist"], ["半导体", "光伏", "消费"])

        photovoltaic = next(item for item in context["candidate_universe"] if item["theme"] == "光伏")
        self.assertEqual(
            [item["fund_code"] for item in photovoltaic["funds"]],
            ["020118", "020119", "020120"],
        )


if __name__ == "__main__":
    unittest.main()
