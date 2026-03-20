from __future__ import annotations

import json
import re
import ssl
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import yaml


try:
    import akshare as ak
except ImportError:  # pragma: no cover - runtime dependency
    ak = None


KEYWORD_GROUPS: list[tuple[str, tuple[str, ...]]] = [
    ("qdii", ("QDII", "纳斯达克", "标普", "道琼斯", "恒生", "日经", "全球", "海外", "美股")),
    ("bond", ("债", "纯债", "短债", "中短债", "金融债", "国债", "利率债", "信用债")),
    ("gold", ("黄金", "上海金", "贵金属")),
    ("domestic_index", ("沪深", "中证", "上证", "深证", "A500", "科创50", "科创", "央企红利", "红利低波", "中证红利")),
    ("money", ("货币",)),
    ("fof", ("FOF",)),
    (
        "active_equity",
        (
            "股票", "混合", "成长", "价值", "科技", "机器人", "有色", "人工智能", "AI", "算力",
            "半导体", "芯片", "石油", "油气", "能源", "消费", "航天", "卫星", "储能", "电力", "养老",
        ),
    ),
]


MARKET_SCAN_CATEGORY = "全部"

CORE_CANDIDATE_UNIVERSE_SPECS = [
    {
        "theme": "美股宽基",
        "bucket": "海外核心权益",
        "keywords": ("标普500", "纳斯达克100", "纳斯达克", "标普"),
        "role": "从全市场基金中观察美国大盘与科技主导资产是否仍值得保留或新增暴露。",
        "focus_level": "core",
    },
    {
        "theme": "债券防御",
        "bucket": "防御资产",
        "keywords": ("中短债", "纯债", "金融债", "国债", "利率债", "信用债"),
        "role": "从全市场基金中观察低波动资产是否需要承担组合缓冲与现金管理角色。",
        "focus_level": "core",
    },
    {
        "theme": "A股宽基",
        "bucket": "国内核心权益",
        "keywords": ("沪深300", "中证A500", "A500", "上证50", "中证800"),
        "role": "从全市场基金中观察国内核心权益是否具备承接新增资金的基础配置价值。",
        "focus_level": "core",
    },
    {
        "theme": "红利低波",
        "bucket": "风格因子",
        "keywords": ("红利低波", "中证红利", "红利", "央企红利", "高股息"),
        "role": "从全市场基金中观察现金流、分红与低波风格是否更适合当前市场环境。",
        "focus_level": "core",
    },
]

PRIORITY_INDUSTRY_THEME_SPECS = [
    {
        "theme": "石油能源",
        "bucket": "重点行业",
        "keywords": ("石油", "油气", "原油", "能源"),
        "role": "重点观察石油与油气产业链在通胀、地缘和供给扰动下的配置价值。",
        "focus_level": "priority",
    },
    {
        "theme": "有色金属",
        "bucket": "重点行业",
        "keywords": ("有色", "有色金属", "铜", "铝", "稀土"),
        "role": "重点观察有色金属与资源品在资本开支、供给约束和顺周期修复中的弹性。",
        "focus_level": "priority",
    },
    {
        "theme": "算力基础设施",
        "bucket": "重点行业",
        "keywords": ("算力", "云计算", "数据中心", "通信", "CPO", "服务器"),
        "role": "重点观察算力基础设施在 AI 资本开支扩张中的承接能力。",
        "focus_level": "priority",
    },
    {
        "theme": "人工智能",
        "bucket": "重点行业",
        "keywords": ("人工智能", "AI", "智能驾驶", "大模型"),
        "role": "重点观察 AI 主线是否具备独立于泛科技主题的中期配置理由。",
        "focus_level": "priority",
    },
    {
        "theme": "商业航天",
        "bucket": "重点行业",
        "keywords": ("商业航天", "航天", "卫星", "航空航天"),
        "role": "重点观察商业航天与卫星互联网是否进入更可持续的产业验证阶段。",
        "focus_level": "priority",
    },
    {
        "theme": "电力协同",
        "bucket": "重点行业",
        "keywords": ("电力", "电网", "储能", "虚拟电厂", "智能电网", "电力设备"),
        "role": "重点观察发电、储能、电网与电力设备协同链条的配置机会。",
        "focus_level": "priority",
    },
    {
        "theme": "黄金贵金属",
        "bucket": "重点行业",
        "keywords": ("黄金ETF联接", "黄金", "贵金属", "上海金"),
        "role": "重点观察黄金与贵金属在避险、通胀和美元实际利率变化下的对冲价值。",
        "focus_level": "priority",
    },
    {
        "theme": "养老",
        "bucket": "重点行业",
        "keywords": ("养老", "养老目标", "养老2035", "养老2040", "养老2050", "养老FOF"),
        "role": "重点观察养老目标与养老 FOF 是否适合作为长期稳健资金的承接方向。",
        "focus_level": "priority",
    },
    {
        "theme": "机器人",
        "bucket": "重点行业",
        "keywords": ("机器人", "自动化", "工业母机", "智能制造"),
        "role": "重点观察机器人与自动化是否仍处于高景气但可承受的配置区间。",
        "focus_level": "priority",
    },
    {
        "theme": "半导体",
        "bucket": "重点行业",
        "keywords": ("半导体", "芯片", "集成电路", "科创芯片"),
        "role": "重点观察半导体链条在国产替代与全球资本开支周期中的配置价值。",
        "focus_level": "priority",
    },
    {
        "theme": "消费",
        "bucket": "重点行业",
        "keywords": ("消费", "消费50", "消费电子", "食品饮料", "白酒", "家电", "新消费"),
        "role": "重点观察消费板块在内需修复和盈利改善中的中期机会。",
        "focus_level": "priority",
    },
]

CANDIDATE_UNIVERSE_SPECS = CORE_CANDIDATE_UNIVERSE_SPECS + PRIORITY_INDUSTRY_THEME_SPECS


INDEX_CODES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000300": "沪深300",
}


REGION_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("美国", ("纳斯达克", "标普", "道琼斯", "美股")),
    ("中国", ("沪深", "上证", "深证", "中证", "A500", "科创", "央企", "红利")),
    ("商品/贵金属", ("黄金", "贵金属", "上海金", "油气", "有色", "资源")),
    ("全球/其他海外", ("全球", "海外", "QDII", "恒生", "日经")),
]


BENCHMARK_FAMILY_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("纳斯达克100", ("纳斯达克100", "纳指100", "纳斯达克")),
    ("标普500", ("标普500", "标普")),
    ("沪深300", ("沪深300",)),
    ("中证A500", ("中证A500", "A500")),
    ("红利", ("红利低波", "中证红利", "红利", "央企红利")),
    ("黄金贵金属", ("黄金", "贵金属", "上海金")),
    ("纯债/利率债", ("纯债", "中短债", "短债", "金融债", "国债", "利率债", "信用债", "债券")),
    ("机器人/高端制造", ("机器人", "高端制造", "工业母机", "智能制造")),
    ("资源/油气/有色", ("有色", "油气", "资源", "煤炭")),
]


@dataclass
class ResearchSource:
    name: str
    category: str
    homepage: str
    fetch_strategy: str = "reference"
    feed_url: str | None = None
    enabled: bool = True
    note: str = ""


class _AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._href: str | None = None
        self._chunks: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        self._href = attrs_dict.get("href")
        self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            text = data.strip()
            if text:
                self._chunks.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._href is None:
            return
        text = "".join(self._chunks).strip()
        if text:
            self.links.append((self._href, text))
        self._href = None
        self._chunks = []


class ExternalResearchEngine:
    def __init__(self, config_path: Path, cache_dir: Path) -> None:
        self.config_path = config_path
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.priority_watchlist_path = self.cache_dir.parent / "priority_industry_watchlist.json"
        self.sources = self._load_sources()

    def describe_priority_industry_watchlist(self) -> dict[str, Any]:
        defaults = [item["theme"] for item in PRIORITY_INDUSTRY_THEME_SPECS]
        stored = self._load_priority_industry_watchlist_store()
        configured = self._normalize_priority_industry_watchlist(stored.get("active_themes"))
        if not configured:
            stored = {
                "active_themes": defaults,
                "updated_at": stored.get("updated_at"),
            }
            with open(self.priority_watchlist_path, "w", encoding="utf-8") as handle:
                json.dump(stored, handle, ensure_ascii=False, indent=2)
            configured = list(defaults)
        active_themes = configured or defaults
        default_theme_set = set(defaults)
        return {
            "active_themes": active_themes,
            "default_themes": defaults,
            "custom_themes": [theme for theme in active_themes if theme not in default_theme_set],
            "updated_at": stored.get("updated_at"),
            "watchlist_path": str(self.priority_watchlist_path),
        }

    def update_priority_industry_watchlist(self, themes: list[str]) -> dict[str, Any]:
        normalized = self._normalize_priority_industry_watchlist(themes)
        if not normalized:
            normalized = [item["theme"] for item in PRIORITY_INDUSTRY_THEME_SPECS]

        payload = {
            "active_themes": normalized,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        with open(self.priority_watchlist_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return self.describe_priority_industry_watchlist()

    def lookup_fund_trade_constraint(
        self,
        fund_code: str,
        fund_name: str = "",
        ttl_hours: int = 12,
    ) -> dict[str, Any]:
        normalized_code = str(fund_code).strip()
        cache_key = f"fund_trade_constraint_{normalized_code}"
        cached = self._load_json_cache(cache_key, ttl_hours=ttl_hours)
        if cached is not None:
            return cached

        result: dict[str, Any] = {
            "fund_code": normalized_code,
            "fund_name": fund_name,
            "purchase_status": "未知",
            "daily_purchase_limit_amount": None,
            "is_purchase_open": None,
            "constraint_source": "eastmoney",
            "source_url": f"https://fundf10.eastmoney.com/jjfl_{normalized_code}.html",
            "fetch_succeeded": False,
            "auto_maintained": True,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
        }

        try:
            html = self._fetch_url_text(result["source_url"])
            compact = re.sub(r"\s+", "", html)
            if "暂停申购" in compact:
                result["purchase_status"] = "暂停申购"
                result["is_purchase_open"] = False
            elif "限大额" in compact or "限制大额申购" in compact:
                result["purchase_status"] = "限大额"
                result["is_purchase_open"] = True
            elif "开放申购" in compact or "正常申购" in compact:
                result["purchase_status"] = "开放申购"
                result["is_purchase_open"] = True

            limit_match = re.search(
                r"单日累计(?:购买|申购)?上限([0-9]+(?:\.[0-9]+)?)元",
                compact,
            )
            if limit_match:
                result["daily_purchase_limit_amount"] = round(float(limit_match.group(1)), 2)

            result["fetch_succeeded"] = True
        except Exception:
            pass

        self._save_json_cache(cache_key, result)
        return result

    def lookup_fund_purchase_fee_rate(
        self,
        fund_code: str,
        fund_name: str = "",
        ttl_hours: int = 12,
    ) -> float | None:
        normalized_code = str(fund_code).strip()
        if not normalized_code:
            return None

        categories = [MARKET_SCAN_CATEGORY, "指数型", "债券型", "QDII"]
        for category in categories:
            rows = self._load_fund_ranking_rows(category, ttl_hours=ttl_hours)
            for row in rows:
                if str(row.get("基金代码", "")).strip() != normalized_code:
                    continue
                return self._parse_fee_rate_pct(row.get("手续费"))
        return None

    def lookup_fund_settlement_rule(
        self,
        fund_code: str,
        fund_name: str = "",
        ttl_hours: int = 24,
    ) -> dict[str, Any]:
        normalized_code = str(fund_code).strip()
        cache_key = f"fund_settlement_rule_{normalized_code}"
        cached = self._load_json_cache(cache_key, ttl_hours=ttl_hours)
        if isinstance(cached, dict):
            return cached

        rule: dict[str, Any] = {
            "fund_code": normalized_code,
            "fund_name": fund_name,
            "cutoff_time": "15:00",
            "confirm_trade_day_lag": 0,
            "effective_trade_day_lag_after_confirm": 1,
            "rule_source": "inference",
            "fetch_succeeded": False,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
        }
        text = f"{fund_name} {normalized_code}".upper()
        if "QDII" in text or any(token in text for token in ("纳斯达克", "标普", "恒生", "海外", "美股")):
            rule["effective_trade_day_lag_after_confirm"] = 2

        source_url = f"https://fundf10.eastmoney.com/jjfl_{normalized_code}.html"
        try:
            html = self._fetch_url_text(source_url)
            compact = re.sub(r"\s+", "", html)
            cutoff = self._extract_cutoff_time(compact)
            if cutoff is not None:
                rule["cutoff_time"] = cutoff.strftime("%H:%M")
            confirm_lag = self._extract_confirm_trade_day_lag(compact)
            if confirm_lag is not None:
                rule["confirm_trade_day_lag"] = confirm_lag
            effective_lag = self._extract_effective_trade_day_lag(compact)
            if effective_lag is not None:
                rule["effective_trade_day_lag_after_confirm"] = effective_lag
            rule["rule_source"] = "eastmoney+inference"
            rule["fetch_succeeded"] = True
        except Exception:
            pass

        self._save_json_cache(cache_key, rule)
        return rule

    def build_report(
        self,
        snapshot: dict[str, Any],
        mode: str,
        risk_profile: str = "稳健",
        as_of: date | None = None,
    ) -> str:
        report_date = as_of or date.today()
        holdings_summary = self._summarize_holdings(snapshot["positions"])
        source_updates = self._collect_source_updates(days=7, per_source=3)
        market_context = self._collect_market_context()
        candidates = self._build_candidate_universe(snapshot["positions"])

        if mode == "weekly":
            return self._render_weekly_report(
                snapshot=snapshot,
                holdings_summary=holdings_summary,
                source_updates=source_updates,
                market_context=market_context,
                candidates=candidates,
                risk_profile=risk_profile,
                report_date=report_date,
            )
        return self._render_daily_report(
            snapshot=snapshot,
            holdings_summary=holdings_summary,
            source_updates=source_updates,
            market_context=market_context,
            candidates=candidates,
            risk_profile=risk_profile,
            report_date=report_date,
        )

    def build_monthly_briefing_material_packet(
        self,
        snapshot: dict[str, Any],
        available_cash: float | None = None,
        risk_profile: str = "稳健",
        as_of: date | None = None,
    ) -> dict[str, Any]:
        report_date = as_of or date.today()
        holdings_summary = self._summarize_holdings(snapshot["positions"])
        source_updates = self._collect_source_updates(days=30, per_source=4)
        market_context = self._collect_market_context()
        portfolio_diagnostics = self._build_portfolio_diagnostics(snapshot, holdings_summary)
        candidate_context = self._build_candidate_universe_context(snapshot["positions"])
        candidate_universe = candidate_context["candidate_universe"]
        fund_constraints_catalog = self._build_fund_constraints_catalog(
            snapshot,
            candidate_universe,
        )
        news_events = self._flatten_source_events(source_updates)
        news_events.sort(
            key=lambda item: (
                item.get("published_at", ""),
                item.get("category", ""),
                item.get("source_name", ""),
            ),
            reverse=True,
        )

        packet = {
            "report_date": report_date.isoformat(),
            "risk_profile": risk_profile,
            "investor_profile": {
                "style": "长期定投，非短线投机",
                "briefing_preference": "每月总结，不做日报与周报",
                "goal": "结合一个月经济资讯与市场线索，优化基金配置与补仓方向",
                "new_cash_style_preference": "新增资金以 80% 稳健、20% 进攻为主",
            },
            "capital_preferences": {
                "same_day_execution_only": True,
                "must_fully_deploy_available_cash": bool(available_cash is not None and available_cash > 0),
                "stable_target_pct": 80,
                "speculative_target_pct": 20,
                "cash_retention_allowed": False,
            },
            "portfolio_snapshot": snapshot,
            "holdings_summary": holdings_summary,
            "portfolio_diagnostics": portfolio_diagnostics,
            "same_day_execution_context": snapshot.get("same_day_execution_context", {}),
            "fund_constraints_catalog": fund_constraints_catalog,
            "market_context": self._briefing_market_context(market_context),
            "candidate_universe": candidate_universe,
            "candidate_universe_scope": candidate_context["candidate_universe_scope"],
            "priority_industry_watchlist": candidate_context["priority_industry_watchlist"],
            "analysis_window_days": 30,
            "news_events": news_events[:48],
            "source_whitelist": [
                {
                    "name": source.name,
                    "category": source.category,
                    "homepage": source.homepage,
                    "note": source.note,
                }
                for source in self.sources
                if source.enabled
            ],
        }
        if available_cash is not None and available_cash > 0:
            packet["available_cash"] = round(float(available_cash), 2)
            packet["allocation_constraints"] = {
                "currency": "CNY",
                "amount_granularity": 10,
                "must_sum_to_available_cash": True,
                "must_fully_deploy_today": True,
                "same_day_executable_only": True,
                "cash_retention_allowed": False,
                "consider_due_dca_today": True,
                "prefer_stable_pct": 80,
                "prefer_speculative_pct": 20,
                "execution_style": "只生成建议，不自动下单",
            }
        return packet

    def build_cash_deployment_material_packet(
        self,
        snapshot: dict[str, Any],
        available_cash: float,
        latest_monthly_report: dict[str, Any] | None = None,
        risk_profile: str = "稳健",
        as_of: date | None = None,
    ) -> dict[str, Any]:
        report_date = as_of or date.today()
        holdings_summary = self._summarize_holdings(snapshot["positions"])
        portfolio_diagnostics = self._build_portfolio_diagnostics(snapshot, holdings_summary)
        market_context = self._collect_market_context()
        candidate_context = self._build_candidate_universe_context(snapshot["positions"])
        candidate_universe = candidate_context["candidate_universe"]
        fund_constraints_catalog = self._build_fund_constraints_catalog(
            snapshot,
            candidate_universe,
        )

        monthly_context = None
        if latest_monthly_report:
            monthly_context = {
                "id": latest_monthly_report.get("id"),
                "created_at": latest_monthly_report.get("created_at"),
                "report_type": latest_monthly_report.get("report_type"),
                "skill_name": latest_monthly_report.get("skill_name"),
                "report_body": latest_monthly_report.get("report_body"),
            }

        return {
            "report_date": report_date.isoformat(),
            "risk_profile": risk_profile,
            "investor_profile": {
                "style": "长期定投，非短线投机",
                "briefing_preference": "按需生成本次资金调整方案",
                "goal": "基于当前持仓、最新月度研究和可支配现金，给出一次性可执行的金额分配方案",
                "new_cash_style_preference": "新增资金以 80% 稳健、20% 进攻为主",
            },
            "available_cash": round(float(available_cash), 2),
            "allocation_constraints": {
                "currency": "CNY",
                "amount_granularity": 10,
                "must_sum_to_available_cash": True,
                "must_fully_deploy_today": True,
                "same_day_executable_only": True,
                "cash_retention_allowed": False,
                "consider_due_dca_today": True,
                "prefer_stable_pct": 80,
                "prefer_speculative_pct": 20,
                "execution_style": "只生成建议，不自动下单",
            },
            "same_day_execution_context": snapshot.get("same_day_execution_context", {}),
            "fund_constraints_catalog": fund_constraints_catalog,
            "portfolio_snapshot": snapshot,
            "holdings_summary": holdings_summary,
            "portfolio_diagnostics": portfolio_diagnostics,
            "market_context": self._briefing_market_context(market_context),
            "candidate_universe": candidate_universe,
            "candidate_universe_scope": candidate_context["candidate_universe_scope"],
            "priority_industry_watchlist": candidate_context["priority_industry_watchlist"],
            "latest_monthly_report": monthly_context,
        }

    def build_daily_opportunity_material_packet(
        self,
        snapshot: dict[str, Any],
        latest_monthly_report: dict[str, Any] | None = None,
        available_cash: float | None = None,
        risk_profile: str = "稳健",
        as_of: date | None = None,
    ) -> dict[str, Any]:
        report_date = as_of or date.today()
        holdings_summary = self._summarize_holdings(snapshot["positions"])
        portfolio_diagnostics = self._build_portfolio_diagnostics(snapshot, holdings_summary)
        source_updates = self._collect_source_updates(days=5, per_source=3)
        market_context = self._collect_market_context()
        candidate_context = self._build_candidate_universe_context(snapshot["positions"])
        candidate_universe = candidate_context["candidate_universe"]
        fund_constraints_catalog = self._build_fund_constraints_catalog(
            snapshot,
            candidate_universe,
        )
        news_events = self._flatten_source_events(source_updates)
        news_events.sort(
            key=lambda item: (
                item.get("published_at", ""),
                item.get("category", ""),
                item.get("source_name", ""),
            ),
            reverse=True,
        )

        monthly_context = None
        if latest_monthly_report:
            monthly_context = {
                "id": latest_monthly_report.get("id"),
                "created_at": latest_monthly_report.get("created_at"),
                "report_type": latest_monthly_report.get("report_type"),
                "skill_name": latest_monthly_report.get("skill_name"),
                "report_body": latest_monthly_report.get("report_body"),
            }

        packet = {
            "report_date": report_date.isoformat(),
            "risk_profile": risk_profile,
            "investor_profile": {
                "style": "长期定投，非短线投机",
                "briefing_preference": "月报为主，日常只在强信号时行动",
                "goal": "每天监测市场动态，只在出现足够强的例外买点时提醒并给出当日可执行建议",
                "new_cash_style_preference": "新增资金优先承担补核心仓、防御仓或对冲仓的角色",
            },
            "daily_monitor_policy": {
                "default_action": "no_action",
                "exception_only": True,
                "max_funds_per_day": 2,
                "allow_same_day_execution_only": True,
                "requires_clear_why_now": True,
                "prefer_alignment_with_latest_monthly_view": True,
            },
            "allocation_constraints": {
                "currency": "CNY",
                "amount_granularity": 10,
                "same_day_executable_only": True,
                "consider_due_dca_today": True,
                "cash_retention_allowed": True,
            },
            "same_day_execution_context": snapshot.get("same_day_execution_context", {}),
            "fund_constraints_catalog": fund_constraints_catalog,
            "portfolio_snapshot": snapshot,
            "holdings_summary": holdings_summary,
            "portfolio_diagnostics": portfolio_diagnostics,
            "market_context": self._briefing_market_context(market_context),
            "candidate_universe": candidate_universe,
            "candidate_universe_scope": candidate_context["candidate_universe_scope"],
            "priority_industry_watchlist": candidate_context["priority_industry_watchlist"],
            "priority_industry_watch_snapshot": self._build_priority_industry_watch_snapshot(
                candidate_universe,
                market_context,
                news_events[:24],
            ),
            "analysis_window_days": 5,
            "news_events": news_events[:24],
            "latest_monthly_report": monthly_context,
        }
        if available_cash is not None and available_cash > 0:
            packet["available_cash"] = round(float(available_cash), 2)
        return packet

    def _build_fund_constraints_catalog(
        self,
        snapshot: dict[str, Any],
        candidate_universe: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        tracked_limits = snapshot.get("same_day_execution_context", {}).get("tracked_fund_limits", [])
        results: list[dict[str, Any]] = []
        seen_codes: set[str] = set()
        for item in tracked_limits:
            limit_amount = item.get("daily_purchase_limit_amount")
            today_due_dca_amount = float(item.get("today_due_dca_amount", 0.0) or 0.0)
            if limit_amount is None and today_due_dca_amount <= 0:
                continue
            fund_code = item.get("fund_code")
            if fund_code:
                seen_codes.add(str(fund_code))
            results.append(
                {
                    "fund_code": fund_code,
                    "fund_name": item.get("fund_name"),
                    "daily_purchase_limit_amount": limit_amount,
                    "today_due_dca_amount": round(today_due_dca_amount, 2),
                    "today_remaining_purchase_capacity": item.get("today_remaining_purchase_capacity"),
                    "today_limit_exceeded": bool(item.get("today_limit_exceeded")),
                    "purchase_status": item.get("purchase_status", "未知"),
                    "auto_maintained": item.get("auto_maintained", True),
                }
            )

        for candidate in candidate_universe or []:
            for fund in candidate.get("funds", []):
                fund_code = str(fund.get("fund_code", "")).strip()
                if not fund_code or fund_code in seen_codes:
                    continue
                limit_amount = fund.get("daily_purchase_limit_amount")
                purchase_status = fund.get("purchase_status", "未知")
                if limit_amount is None and purchase_status == "未知":
                    continue
                seen_codes.add(fund_code)
                results.append(
                    {
                        "fund_code": fund_code,
                        "fund_name": fund.get("fund_name"),
                        "daily_purchase_limit_amount": limit_amount,
                        "today_due_dca_amount": round(float(fund.get("today_due_dca_amount", 0.0) or 0.0), 2),
                        "today_remaining_purchase_capacity": fund.get("today_remaining_purchase_capacity"),
                        "today_limit_exceeded": bool(fund.get("today_limit_exceeded")),
                        "purchase_status": purchase_status,
                        "auto_maintained": True,
                    }
                )
        return results

    def _load_sources(self) -> list[ResearchSource]:
        if not self.config_path.exists():
            return []
        with open(self.config_path, "r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
        items = raw.get("sources", [])
        sources: list[ResearchSource] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            sources.append(
                ResearchSource(
                    name=str(item.get("name", "")),
                    category=str(item.get("category", "未分类")),
                    homepage=str(item.get("homepage", "")),
                    fetch_strategy=str(item.get("fetch_strategy", "reference")),
                    feed_url=item.get("feed_url"),
                    enabled=bool(item.get("enabled", True)),
                    note=str(item.get("note", "")),
                )
            )
        return sources

    def _cache_path(self, name: str) -> Path:
        return self.cache_dir / f"{name}.json"

    def _load_json_cache(self, name: str, ttl_hours: int | None) -> Any | None:
        path = self._cache_path(name)
        if not path.exists():
            return None
        if ttl_hours is not None:
            age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
            if age > timedelta(hours=ttl_hours):
                return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return None

    def _save_json_cache(self, name: str, payload: Any) -> None:
        path = self._cache_path(name)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)

    def _load_priority_industry_watchlist_store(self) -> dict[str, Any]:
        if not self.priority_watchlist_path.exists():
            return {}
        try:
            with open(self.priority_watchlist_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _normalize_priority_industry_watchlist(self, themes: Any) -> list[str]:
        if not isinstance(themes, list):
            return []

        normalized: list[str] = []
        seen: set[str] = set()
        for item in themes:
            theme = re.sub(r"\s+", " ", str(item or "")).strip()
            if not theme or theme in seen:
                continue
            seen.add(theme)
            normalized.append(theme)
        return normalized

    def _priority_industry_theme_specs(self) -> list[dict[str, Any]]:
        active_themes = self.describe_priority_industry_watchlist()["active_themes"]
        builtin_specs = {
            str(item.get("theme", "")).strip(): item
            for item in PRIORITY_INDUSTRY_THEME_SPECS
            if item.get("theme")
        }

        specs: list[dict[str, Any]] = []
        for theme in active_themes:
            builtin = builtin_specs.get(theme)
            if builtin:
                specs.append(dict(builtin))
                continue
            specs.append(self._build_custom_priority_industry_spec(theme))
        return specs

    def _build_custom_priority_industry_spec(self, theme: str) -> dict[str, Any]:
        keywords = self._derive_custom_priority_theme_keywords(theme)
        return {
            "theme": theme,
            "bucket": "重点行业",
            "keywords": keywords,
            "role": f"重点观察{theme}相关产业链在当前市场环境中的配置价值。",
            "focus_level": "priority",
            "is_custom": True,
        }

    def _derive_custom_priority_theme_keywords(self, theme: str) -> tuple[str, ...]:
        normalized = re.sub(r"\s+", " ", str(theme or "")).strip()
        if not normalized:
            return tuple()

        candidates = [normalized]
        simplified = re.sub(r"(主题|产业|行业|概念|板块)$", "", normalized).strip()
        if simplified and simplified not in candidates:
            candidates.append(simplified)

        for fragment in re.split(r"[、，,/／；;\s]+", normalized):
            token = fragment.strip()
            if len(token) < 2 or token in candidates:
                continue
            candidates.append(token)
        return tuple(candidates)

    def _collect_market_context(self) -> dict[str, Any]:
        if ak is None:
            return {
                "indexes": [],
                "sectors": [],
                "theme_funds": [],
                "skill_output": "Akshare 未安装，无法生成市场上下文。",
            }

        skill_output = self._run_akshare_skill_output()

        indexes: list[dict[str, Any]] = []
        sectors: list[dict[str, Any]] = []
        theme_funds: list[dict[str, Any]] = []
        try:
            index_df = ak.stock_zh_index_spot_em()
            for code, name in INDEX_CODES.items():
                selected = index_df[index_df["代码"] == code]
                if selected.empty:
                    continue
                row = selected.iloc[0]
                indexes.append(
                    {
                        "name": name,
                        "code": code,
                        "latest": float(row["最新价"]),
                        "pct_change": float(row["涨跌幅"]),
                    }
                )
        except Exception as exc:
            skill_output = f"{skill_output}\n\n[指数获取失败] {exc}".strip()

        try:
            sector_df = ak.stock_board_industry_name_em()
            top_rows = sector_df.sort_values("涨跌幅", ascending=False).head(5)
            bottom_rows = sector_df.sort_values("涨跌幅", ascending=True).head(3)
            for _, row in top_rows.iterrows():
                sectors.append(
                    {
                        "direction": "top",
                        "name": str(row["板块名称"]),
                        "pct_change": float(row["涨跌幅"]),
                    }
                )
            for _, row in bottom_rows.iterrows():
                sectors.append(
                    {
                        "direction": "bottom",
                        "name": str(row["板块名称"]),
                        "pct_change": float(row["涨跌幅"]),
                    }
                )
        except Exception as exc:
            skill_output = f"{skill_output}\n\n[板块获取失败] {exc}".strip()
            theme_funds = self._fallback_theme_funds()

        if not indexes and not sectors and not theme_funds:
            theme_funds = self._fallback_theme_funds()

        return {
            "indexes": indexes,
            "sectors": sectors,
            "theme_funds": theme_funds,
            "skill_output": skill_output,
        }

    def _run_akshare_skill_output(self) -> str:
        skill_script = Path.home() / ".agents" / "skills" / "akshare" / "akshare_tool.py"
        if not skill_script.exists():
            return ""
        import subprocess
        import sys

        sections: list[str] = []
        for args in (["--mode", "index-overview"], ["--mode", "sector-top"]):
            try:
                result = subprocess.run(
                    [sys.executable, str(skill_script), *args],
                    capture_output=True,
                    text=True,
                    timeout=45,
                    check=False,
                )
                if result.returncode == 0 and result.stdout.strip():
                    sections.append(result.stdout.strip())
            except Exception:
                continue
        return "\n\n".join(sections)

    def _fallback_theme_funds(self) -> list[dict[str, Any]]:
        if ak is None:
            return []
        try:
            df = ak.fund_open_fund_rank_em(symbol="指数型")
        except Exception:
            return []
        filtered = (
            df[df["基金简称"].astype(str).str.contains("中证|沪深|上证|红利|纳斯达克|标普|黄金", na=False)]
            .sort_values("近1月", ascending=False)
            .head(5)
        )
        results: list[dict[str, Any]] = []
        for _, row in filtered.iterrows():
            results.append(
                {
                    "fund_name": str(row["基金简称"]),
                    "fund_code": str(row["基金代码"]),
                    "one_month": self._safe_float(row.get("近1月")),
                }
            )
        return results

    def _collect_source_updates(self, days: int, per_source: int) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        for source in self.sources:
            if not source.enabled:
                continue

            items: list[dict[str, Any]] = []
            if source.fetch_strategy == "rss" and source.feed_url:
                items = self._fetch_rss_items(source, cutoff, per_source)
            elif source.fetch_strategy == "html_links":
                items = self._fetch_html_link_items(source, per_source, cutoff)

            if items:
                collected.append(
                    {
                        "name": source.name,
                        "category": source.category,
                        "items": items,
                        "fetch_strategy": source.fetch_strategy,
                    }
                )
        return collected

    def _fetch_rss_items(
        self,
        source: ResearchSource,
        cutoff: datetime,
        per_source: int,
    ) -> list[dict[str, str]]:
        try:
            xml_bytes = self._fetch_url_bytes(source.feed_url or source.homepage)
            root = ET.fromstring(xml_bytes)
        except Exception:
            return []

        items: list[dict[str, str]] = []
        tag_name = root.tag.lower()
        if tag_name.endswith("rss") or "rss" in tag_name:
            nodes = root.findall(".//item")
            for node in nodes[: per_source * 4]:
                title = (node.findtext("title") or "").strip()
                link = (node.findtext("link") or "").strip()
                published = self._parse_datetime(
                    node.findtext("pubDate") or node.findtext("date") or node.findtext("updated")
                )
                if not title or not link:
                    continue
                if published and published.astimezone(timezone.utc) < cutoff:
                    continue
                items.append(
                    {
                        "title": title,
                        "link": link,
                        "published_at": published.isoformat() if published else "",
                    }
                )
                if len(items) >= per_source:
                    break
        else:
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            nodes = root.findall(".//atom:entry", ns) or root.findall(".//entry")
            for node in nodes[: per_source * 4]:
                title = (node.findtext("{http://www.w3.org/2005/Atom}title") or node.findtext("title") or "").strip()
                published = self._parse_datetime(
                    node.findtext("{http://www.w3.org/2005/Atom}updated")
                    or node.findtext("{http://www.w3.org/2005/Atom}published")
                    or node.findtext("updated")
                    or node.findtext("published")
                )
                link = ""
                for candidate in node.findall("{http://www.w3.org/2005/Atom}link") + node.findall("link"):
                    link = candidate.attrib.get("href", "").strip()
                    if link:
                        break
                if not title or not link:
                    continue
                if published and published.astimezone(timezone.utc) < cutoff:
                    continue
                items.append(
                    {
                        "title": title,
                        "link": link,
                        "published_at": published.isoformat() if published else "",
                    }
                )
                if len(items) >= per_source:
                    break
        return items

    def _fetch_html_link_items(
        self,
        source: ResearchSource,
        per_source: int,
        cutoff: datetime | None = None,
    ) -> list[dict[str, str]]:
        try:
            html = self._fetch_url_text(source.homepage)
        except Exception:
            return []

        parser = _AnchorCollector()
        parser.feed(html)

        banned = {"首页", "更多", "登录", "注册", "English", "下载", "关于我们", "联系我们"}
        banned_fragments = (
            "english version",
            "var ",
            "market data home",
            "markets home",
            "contact",
            "about",
            "一件事",
            "专栏",
            "global",
        )
        items: list[dict[str, str]] = []
        seen_titles: set[str] = set()
        for href, text in parser.links:
            normalized = re.sub(r"\s+", " ", text).strip()
            normalized = re.sub(r"(..{8,}?)\1+", r"\1", normalized)
            if len(normalized) > 110:
                normalized = f"{normalized[:110]}..."
            lowered = normalized.lower()
            if (
                not normalized
                or normalized in banned
                or len(normalized) < 6
                or any(fragment in lowered for fragment in banned_fragments)
                or normalized in seen_titles
            ):
                continue
            if href.startswith("javascript:") or href.startswith("#") or href in {"/", "./"}:
                continue
            absolute_link = urljoin(source.homepage, href)
            published = self._infer_datetime_from_text_or_url(normalized, absolute_link)
            if published and cutoff and published.astimezone(timezone.utc) < cutoff:
                continue
            seen_titles.add(normalized)
            items.append(
                {
                    "title": normalized,
                    "link": absolute_link,
                    "published_at": published.isoformat() if published else "",
                }
            )
            if len(items) >= per_source:
                break
        return items

    def _fetch_url_bytes(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": "FundTracker/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.read()
        except urllib.error.URLError as exc:
            if not isinstance(exc.reason, ssl.SSLCertVerificationError):
                raise
            insecure_context = ssl.create_default_context()
            insecure_context.check_hostname = False
            insecure_context.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(request, timeout=20, context=insecure_context) as response:
                return response.read()
        except ssl.SSLCertVerificationError:
            insecure_context = ssl.create_default_context()
            insecure_context.check_hostname = False
            insecure_context.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(request, timeout=20, context=insecure_context) as response:
                return response.read()

    def _fetch_url_text(self, url: str) -> str:
        data = self._fetch_url_bytes(url)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            try:
                return data.decode("gb18030")
            except UnicodeDecodeError:
                return data.decode("utf-8", errors="ignore")

    def _infer_datetime_from_text_or_url(self, text: str, url: str) -> datetime | None:
        patterns = [
            r"(20\d{2})[-/年](\d{1,2})[-/月](\d{1,2})",
            r"(20\d{2})(\d{2})(\d{2})",
        ]
        combined = f"{text} {url}"
        for pattern in patterns:
            match = re.search(pattern, combined)
            if not match:
                continue
            year, month, day = (int(part) for part in match.groups())
            try:
                return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _parse_datetime(self, raw: str | None) -> datetime | None:
        if not raw:
            return None
        text = raw.strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            return datetime.fromisoformat(text)
        except ValueError:
            try:
                return parsedate_to_datetime(text)
            except Exception:
                return None

    def _summarize_holdings(self, positions: list[dict[str, Any]]) -> dict[str, Any]:
        total_market_value = sum(float(item["market_value"]) for item in positions)
        bucket_weights: dict[str, float] = {}
        enriched_positions: list[dict[str, Any]] = []

        for item in positions:
            category = self._classify_position(item["fund_name"])
            market_value = float(item["market_value"])
            bucket_weights[category] = bucket_weights.get(category, 0.0) + market_value
            enriched_positions.append({**item, "exposure_category": category})

        distribution = {
            category: round(value / total_market_value * 100, 2) if total_market_value else 0
            for category, value in bucket_weights.items()
        }
        primary_category = max(distribution.items(), key=lambda pair: pair[1])[0] if distribution else "unknown"
        top_position = max(positions, key=lambda item: item["weight_pct"]) if positions else None
        return {
            "distribution": distribution,
            "primary_category": primary_category,
            "top_position": top_position,
            "positions": enriched_positions,
        }

    def _classify_position(self, fund_name: str) -> str:
        for category, keywords in KEYWORD_GROUPS:
            if any(keyword in fund_name for keyword in keywords):
                return category
        return "other"

    def _match_rule_label(
        self,
        text: str,
        rules: list[tuple[str, tuple[str, ...]]],
        fallback: str,
    ) -> str:
        for label, keywords in rules:
            if any(keyword in text for keyword in keywords):
                return label
        return fallback

    def _build_portfolio_diagnostics(
        self,
        snapshot: dict[str, Any],
        holdings_summary: dict[str, Any],
    ) -> dict[str, Any]:
        positions = snapshot.get("positions", [])
        dca_plans = snapshot.get("active_dca_plans", [])

        sorted_positions = sorted(
            positions,
            key=lambda item: float(item.get("weight_pct", 0.0)),
            reverse=True,
        )
        top_2_weight = round(sum(float(item.get("weight_pct", 0.0)) for item in sorted_positions[:2]), 2)
        hhi = round(
            sum((float(item.get("weight_pct", 0.0)) / 100) ** 2 for item in sorted_positions),
            4,
        )

        region_distribution: dict[str, float] = {}
        benchmark_distribution: dict[str, float] = {}
        overlap_groups: list[dict[str, Any]] = []

        grouped_by_family: dict[str, list[dict[str, Any]]] = {}
        for position in positions:
            fund_name = str(position.get("fund_name", ""))
            weight_pct = float(position.get("weight_pct", 0.0))
            region = self._match_rule_label(fund_name, REGION_RULES, "未识别")
            benchmark_family = self._match_rule_label(fund_name, BENCHMARK_FAMILY_RULES, "其他")

            region_distribution[region] = round(region_distribution.get(region, 0.0) + weight_pct, 2)
            benchmark_distribution[benchmark_family] = round(
                benchmark_distribution.get(benchmark_family, 0.0) + weight_pct,
                2,
            )
            grouped_by_family.setdefault(benchmark_family, []).append(position)

        for family, items in grouped_by_family.items():
            total_weight = round(sum(float(item.get("weight_pct", 0.0)) for item in items), 2)
            if len(items) >= 2 or total_weight >= 35:
                overlap_groups.append(
                    {
                        "family": family,
                        "total_weight_pct": total_weight,
                        "fund_codes": [str(item.get("fund_code", "")) for item in items],
                        "fund_names": [str(item.get("fund_name", "")) for item in items],
                    }
                )

        overlap_groups.sort(key=lambda item: item["total_weight_pct"], reverse=True)

        dca_region_distribution: dict[str, float] = {}
        dca_family_distribution: dict[str, float] = {}
        total_dca_amount = sum(float(plan.get("amount", 0.0)) for plan in dca_plans)
        for plan in dca_plans:
            fund_name = str(plan.get("fund_name", ""))
            amount = float(plan.get("amount", 0.0))
            region = self._match_rule_label(fund_name, REGION_RULES, "未识别")
            benchmark_family = self._match_rule_label(fund_name, BENCHMARK_FAMILY_RULES, "其他")
            dca_region_distribution[region] = round(dca_region_distribution.get(region, 0.0) + amount, 2)
            dca_family_distribution[benchmark_family] = round(
                dca_family_distribution.get(benchmark_family, 0.0) + amount,
                2,
            )

        facts: list[str] = []
        distribution = holdings_summary.get("distribution", {})
        if distribution:
            parts = [
                f"{self._display_category_name(category)} {weight:.2f}%"
                for category, weight in sorted(distribution.items(), key=lambda item: item[1], reverse=True)
            ]
            facts.append("当前资产类别分布：" + "；".join(parts) + "。")
        if sorted_positions:
            facts.append(
                f"前两大持仓合计占比 {top_2_weight:.2f}%，第一大持仓为 "
                f"{sorted_positions[0]['fund_name']}（{sorted_positions[0]['weight_pct']:.2f}%）。"
            )
        if overlap_groups:
            top_group = overlap_groups[0]
            facts.append(
                f"最集中的基准族为 {top_group['family']}，合计占比 {top_group['total_weight_pct']:.2f}%，"
                f"涉及 {'、'.join(top_group['fund_codes'])}。"
            )
        if total_dca_amount > 0:
            dca_parts = [
                f"{label} {round(amount / total_dca_amount * 100, 2):.2f}%"
                for label, amount in sorted(dca_region_distribution.items(), key=lambda item: item[1], reverse=True)
            ]
            facts.append("当前定投资金地域分布：" + "；".join(dca_parts) + "。")

        return {
            "concentration": {
                "position_count": len(sorted_positions),
                "top_1_weight_pct": round(float(sorted_positions[0].get("weight_pct", 0.0)), 2)
                if sorted_positions
                else 0.0,
                "top_2_weight_pct": top_2_weight,
                "hhi": hhi,
            },
            "region_distribution": region_distribution,
            "benchmark_distribution": benchmark_distribution,
            "active_dca": {
                "plan_count": len(dca_plans),
                "total_amount": round(total_dca_amount, 2),
                "region_distribution_amount": dca_region_distribution,
                "benchmark_distribution_amount": dca_family_distribution,
            },
            "overlap_groups": overlap_groups,
            "facts": facts,
        }

    def _build_candidate_universe(
        self,
        positions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return self._build_candidate_universe_context(positions)["candidate_universe"]

    def _build_candidate_universe_context(
        self,
        positions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        current_codes = {str(item.get("fund_code", "")).strip() for item in positions if item.get("fund_code")}
        all_market_rows = self._load_fund_ranking_rows(MARKET_SCAN_CATEGORY)
        excluded_codes = set(current_codes)
        priority_specs = self._priority_industry_theme_specs()

        pool: list[dict[str, Any]] = []
        for rule in CORE_CANDIDATE_UNIVERSE_SPECS + priority_specs:
            items = self._search_funds(
                keywords=rule["keywords"],
                category=MARKET_SCAN_CATEGORY,
                exclude_codes=excluded_codes,
                limit=3 if rule.get("focus_level") == "priority" else 2,
                ranking_rows=all_market_rows,
            )
            if items:
                excluded_codes.update(str(item["fund_code"]) for item in items if item.get("fund_code"))
                pool.append(
                    {
                        "theme": rule["theme"],
                        "bucket": rule["bucket"],
                        "role": rule["role"],
                        "reason": rule["role"],
                        "focus_level": rule.get("focus_level", "priority"),
                        "selection_scope": "全市场开放式基金扫描",
                        "funds": items,
                    }
                )
        return {
            "candidate_universe": pool,
            "candidate_universe_scope": {
                "source_category": MARKET_SCAN_CATEGORY,
                "selection_method": "先扫描全市场开放式基金，再按核心补位主题和重点行业压缩为候选摘要。",
                "total_funds_scanned": len(all_market_rows),
                "excluded_current_holding_count": len(current_codes),
            },
            "priority_industry_watchlist": [item["theme"] for item in priority_specs],
        }

    def _build_priority_industry_watch_snapshot(
        self,
        candidate_universe: list[dict[str, Any]],
        market_context: dict[str, Any],
        news_events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        candidates_by_theme = {
            str(item.get("theme", "")).strip(): item
            for item in candidate_universe
            if isinstance(item, dict) and item.get("theme")
        }
        sectors = [item for item in market_context.get("sectors", []) if isinstance(item, dict)]
        theme_funds = [item for item in market_context.get("theme_funds", []) if isinstance(item, dict)]
        normalized_news = [item for item in news_events if isinstance(item, dict)]

        results: list[dict[str, Any]] = []
        for spec in self._priority_industry_theme_specs():
            keywords = tuple(str(item) for item in spec.get("keywords", ()))
            matched_candidate = candidates_by_theme.get(spec["theme"], {})
            matched_funds = [
                item for item in matched_candidate.get("funds", [])
                if isinstance(item, dict)
            ][:3]
            top_sector_hits = [
                item for item in sectors
                if item.get("direction") == "top" and self._contains_any_keyword(str(item.get("name", "")), keywords)
            ][:2]
            bottom_sector_hits = [
                item for item in sectors
                if item.get("direction") == "bottom" and self._contains_any_keyword(str(item.get("name", "")), keywords)
            ][:2]
            leading_fund_hits = [
                item for item in theme_funds
                if self._contains_any_keyword(str(item.get("fund_name", "")), keywords)
            ][:2]
            news_hits = [
                item for item in normalized_news
                if self._contains_any_keyword(str(item.get("title", "")), keywords)
            ][:2]

            signal = "neutral"
            if top_sector_hits and not bottom_sector_hits:
                signal = "positive"
            elif bottom_sector_hits and not top_sector_hits:
                signal = "negative"
            elif top_sector_hits and bottom_sector_hits:
                signal = "mixed"
            elif leading_fund_hits or news_hits:
                signal = "positive"

            summary_parts: list[str] = []
            if top_sector_hits:
                summary_parts.append(
                    "板块偏强：" + "、".join(
                        f"{item['name']}({float(item.get('pct_change', 0.0)):+.2f}%)"
                        for item in top_sector_hits
                    )
                )
            if bottom_sector_hits:
                summary_parts.append(
                    "板块承压：" + "、".join(
                        f"{item['name']}({float(item.get('pct_change', 0.0)):+.2f}%)"
                        for item in bottom_sector_hits
                    )
                )
            if leading_fund_hits:
                summary_parts.append(
                    "强势基金线索："
                    + "、".join(
                        f"{item.get('fund_name')}({float(item.get('one_month', 0.0)):+.2f}%)"
                        for item in leading_fund_hits
                        if item.get("fund_name")
                    )
                )
            if news_hits:
                summary_parts.append(
                    "近5天事件："
                    + "；".join(self._short_report_title(str(item.get("title", ""))) for item in news_hits)
                )
            if not summary_parts:
                summary_parts.append("今天公开线索不多，先维持观察。")

            results.append(
                {
                    "theme": spec["theme"],
                    "role": spec["role"],
                    "signal": signal,
                    "today_summary": " ".join(summary_parts),
                    "representative_funds": [
                        {
                            "fund_code": str(fund.get("fund_code", "")).strip(),
                            "fund_name": str(fund.get("fund_name", "")).strip(),
                            "daily_growth_pct": self._safe_float(fund.get("daily_growth_pct")),
                            "one_week": self._safe_float(fund.get("one_week")),
                            "one_month": self._safe_float(fund.get("one_month")),
                            "purchase_status": str(fund.get("purchase_status", "未知")).strip() or "未知",
                            "daily_purchase_limit_amount": fund.get("daily_purchase_limit_amount"),
                            "today_due_dca_amount": self._safe_float(fund.get("today_due_dca_amount")),
                            "today_remaining_purchase_capacity": fund.get("today_remaining_purchase_capacity"),
                        }
                        for fund in matched_funds
                    ],
                }
            )
        return results

    def _load_fund_ranking_rows(
        self,
        category: str,
        ttl_hours: int = 12,
    ) -> list[dict[str, Any]]:
        cache_key = f"fund_rank_{category}"
        ranking_rows = self._load_json_cache(cache_key, ttl_hours=ttl_hours)
        if ranking_rows is None and ak is not None:
            df = ak.fund_open_fund_rank_em(symbol=category)
            ranking_rows = df.to_dict("records")
            self._save_json_cache(cache_key, ranking_rows)
        if ranking_rows is None and ak is None:
            ranking_rows = self._load_json_cache(cache_key, ttl_hours=None)
        if ranking_rows is None:
            return []
        if not isinstance(ranking_rows, list):
            return []
        return [row for row in ranking_rows if isinstance(row, dict)]

    def _search_funds(
        self,
        keywords: tuple[str, ...],
        category: str,
        exclude_codes: set[str],
        limit: int,
        ranking_rows: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        ranking_rows = ranking_rows or self._load_fund_ranking_rows(category)

        results: list[dict[str, Any]] = []
        seen_codes: set[str] = set()
        for row in ranking_rows:
            code = str(row.get("基金代码", "")).strip()
            name = str(row.get("基金简称", "")).strip()
            if (
                not code
                or not name
                or code in exclude_codes
                or code in seen_codes
                or "后端" in name
                or name.endswith("C")
            ):
                continue
            if not any(keyword in name for keyword in keywords):
                continue
            seen_codes.add(code)
            trade_constraint = self.lookup_fund_trade_constraint(code, name)
            inferred_category = self._classify_position(name)
            results.append(
                {
                    "fund_code": code,
                    "fund_name": name,
                    "category": self._display_category_name(inferred_category),
                    "market_category": inferred_category,
                    "price_date": str(row.get("日期", "")) if row.get("日期") is not None else "",
                    "daily_growth_pct": self._safe_float(row.get("日增长率")),
                    "one_week": self._safe_float(row.get("近1周")),
                    "one_month": self._safe_float(row.get("近1月")),
                    "one_year": self._safe_float(row.get("近1年")),
                    "fee": str(row.get("手续费", "")),
                    "purchase_status": trade_constraint.get("purchase_status", "未知"),
                    "daily_purchase_limit_amount": trade_constraint.get("daily_purchase_limit_amount"),
                    "today_due_dca_amount": 0.0,
                    "today_remaining_purchase_capacity": trade_constraint.get("daily_purchase_limit_amount"),
                    "today_limit_exceeded": False,
                    "auto_maintained": True,
                }
            )
            if len(results) >= limit:
                break
        return results

    def _safe_float(self, value: Any) -> float | None:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except Exception:
            return None

    def _parse_fee_rate_pct(self, value: Any) -> float | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        matched = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%", text)
        if matched:
            try:
                return round(float(matched.group(1)), 4)
            except ValueError:
                return None
        return self._safe_float(text)

    def _extract_cutoff_time(self, compact_text: str) -> time | None:
        patterns = [
            r"(?:交易日)?([01]?\d:[0-5]\d)前(?:申购|买入|提交)",
            r"(?:申购|买入)截止(?:时间)?([01]?\d:[0-5]\d)",
        ]
        for pattern in patterns:
            matched = re.search(pattern, compact_text)
            if not matched:
                continue
            try:
                hour_text, minute_text = matched.group(1).split(":")
                return time(hour=int(hour_text), minute=int(minute_text))
            except Exception:
                continue
        return None

    def _extract_confirm_trade_day_lag(self, compact_text: str) -> int | None:
        patterns = [
            r"T\+([0-3])(?:个?交易日|个?工作日|日)?(?:确认|确认份额)",
            r"(?:确认份额|份额确认).*?T\+([0-3])",
        ]
        for pattern in patterns:
            matched = re.search(pattern, compact_text)
            if not matched:
                continue
            try:
                return int(matched.group(1))
            except ValueError:
                continue
        return None

    def _extract_effective_trade_day_lag(self, compact_text: str) -> int | None:
        patterns = [
            r"确认后T\+([1-4])(?:个?交易日|个?工作日|日)?.{0,10}(?:开始计算收益|开始享有收益|收益起算)",
            r"T\+([1-4])(?:个?交易日|个?工作日|日)?.{0,10}(?:开始计算收益|开始享有收益|收益起算)",
            r"确认后下(?:一|1)个交易日",
        ]
        for pattern in patterns:
            matched = re.search(pattern, compact_text)
            if not matched:
                continue
            if matched.lastindex is None:
                return 1
            try:
                return int(matched.group(1))
            except ValueError:
                continue
        return None

    def _contains_any_keyword(self, text: str, keywords: tuple[str, ...]) -> bool:
        lowered = text.lower()
        return any(keyword.lower() in lowered for keyword in keywords if keyword)

    def _render_daily_report(
        self,
        snapshot: dict[str, Any],
        holdings_summary: dict[str, Any],
        source_updates: list[dict[str, Any]],
        market_context: dict[str, Any],
        candidates: list[dict[str, Any]],
        risk_profile: str,
        report_date: date,
    ) -> str:
        lines = ["配置与操作建议", ""]
        lines.extend(self._allocation_and_action_lines(snapshot["positions"], holdings_summary))
        lines.extend(["", "市场概览与候选基金", ""])
        lines.extend(self._market_and_candidate_lines(market_context, candidates))
        lines.extend(["", "近 7 天资讯系统综述", ""])
        lines.extend(
            self._source_summary_lines(
                source_updates,
                market_context,
                holdings_summary,
                candidates,
                weekly=False,
            )
        )
        return "\n".join(lines)

    def _render_weekly_report(
        self,
        snapshot: dict[str, Any],
        holdings_summary: dict[str, Any],
        source_updates: list[dict[str, Any]],
        market_context: dict[str, Any],
        candidates: list[dict[str, Any]],
        risk_profile: str,
        report_date: date,
    ) -> str:
        lines = ["配置、趋势与操作建议", ""]
        lines.extend(self._allocation_and_action_lines(snapshot["positions"], holdings_summary, include_trend=True))
        lines.extend(["", "市场概览与候选基金", ""])
        lines.extend(self._market_and_candidate_lines(market_context, candidates, detailed=True))
        lines.extend(["", "近 7 天资讯系统综述", ""])
        lines.extend(
            self._source_summary_lines(
                source_updates,
                market_context,
                holdings_summary,
                candidates,
                weekly=True,
            )
        )
        return "\n".join(lines)

    def _allocation_and_action_lines(
        self,
        positions: list[dict[str, Any]],
        holdings_summary: dict[str, Any],
        include_trend: bool = False,
    ) -> list[str]:
        lines: list[str] = []
        distribution = holdings_summary["distribution"]
        if distribution:
            lines.append(
                "- 当前资产暴露："
                + "；".join(
                    f"{self._display_category_name(category)} {weight:.2f}%"
                    for category, weight in sorted(distribution.items(), key=lambda item: item[1], reverse=True)
                )
            )

        top_position = holdings_summary["top_position"]
        if top_position and top_position["weight_pct"] >= 35:
            lines.append(
                f"- 集中度偏高：第一大持仓 {top_position['fund_name']} 占比 {top_position['weight_pct']:.2f}%，"
                "当前更适合用新增资金补其他资产，而不是继续单边加仓。"
            )
        if distribution.get("qdii", 0.0) >= 60:
            lines.append("- 当前组合明显偏向海外权益，美股指数波动会主导净值变化，不符合稳健型的分散目标。")
        if distribution.get("bond", 0.0) < 20:
            lines.append("- 债券类资产偏少，组合缺少回撤缓冲，遇到权益波动时抗震能力不足。")
        if distribution.get("gold", 0.0) < 10:
            lines.append("- 黄金类对冲仓位不足，面对通胀、美元和避险情绪变化时弹性不够。")
        if distribution.get("domestic_index", 0.0) < 20:
            lines.append("- A 股宽基/红利底仓不足，当前配置对单一海外市场依赖仍然偏高。")

        if include_trend:
            lines.extend(self._trend_alignment_lines(holdings_summary))

        lines.extend(self._build_action_lines(positions, holdings_summary))
        return lines or ["- 暂未生成有效的配置与操作建议。"]

    def _build_action_lines(
        self,
        positions: list[dict[str, Any]],
        holdings_summary: dict[str, Any],
    ) -> list[str]:
        lines: list[str] = []
        distribution = holdings_summary["distribution"]
        top_position = holdings_summary["top_position"]

        if distribution.get("qdii", 0.0) >= 60:
            lines.append("- 动作建议：控制 QDII 新增仓位，短期不建议继续单边追高纳指/标普方向。")
        else:
            lines.append("- 动作建议：海外权益暴露暂未失衡，可继续观察美元流动性与美股波动。")

        if distribution.get("bond", 0.0) < 20:
            lines.append("- 加仓方向：优先补债券基金，作为稳健型组合的第一层波动缓冲。")
        if distribution.get("gold", 0.0) < 10:
            lines.append("- 加仓方向：黄金可小比例试探性配置，用来对冲宏观不确定性。")
        if distribution.get("domestic_index", 0.0) < 20:
            lines.append("- 加仓方向：补充 A 股宽基或红利低波方向，降低单一海外市场集中度。")

        if top_position and top_position["weight_pct"] >= 35:
            lines.append(
                f"- 仓位控制：第一大持仓 {top_position['fund_name']} 占比 {top_position['weight_pct']:.2f}%，"
                "建议通过新增其他资产来稀释，而不是继续抬高集中度。"
            )

        drawdowns = [
            item for item in positions
            if item.get("daily_pct_change") is not None and float(item["daily_pct_change"]) <= -1.5
        ]
        if drawdowns:
            codes = "、".join(item["fund_code"] for item in drawdowns)
            lines.append(f"- 节奏建议：{codes} 当日跌幅较大，但补仓前应先确认整体配置是否已经失衡。")

        return lines or ["- 暂无明确动作建议。"]

    def _market_lines(self, market_context: dict[str, Any], detailed: bool = False) -> list[str]:
        lines: list[str] = []
        indexes = market_context.get("indexes", [])
        if indexes:
            lines.append(
                "- 主要指数："
                + "；".join(
                    f"{item['name']} {item['pct_change']:+.2f}%（{item['latest']:.2f}）"
                    for item in indexes
                )
            )
        sectors = market_context.get("sectors", [])
        theme_funds = market_context.get("theme_funds", [])
        if sectors:
            top_sectors = [item for item in sectors if item["direction"] == "top"][:5]
            bottom_sectors = [item for item in sectors if item["direction"] == "bottom"][:3]
            if top_sectors:
                lines.append(
                    "- 领涨板块："
                    + "、".join(f"{item['name']}({item['pct_change']:+.2f}%)" for item in top_sectors)
                )
            if bottom_sectors:
                lines.append(
                    "- 走弱板块："
                    + "、".join(f"{item['name']}({item['pct_change']:+.2f}%)" for item in bottom_sectors)
                )
        if theme_funds:
            lines.append(
                "- 近期强势基金方向："
                + "、".join(
                    f"{item['fund_name']}({item['one_month']:+.2f}% / {item['fund_code']})"
                    if item["one_month"] is not None else f"{item['fund_name']}({item['fund_code']})"
                    for item in theme_funds
                )
            )
        return lines or ["- 暂未获取到市场上下文。"]

    def _market_and_candidate_lines(
        self,
        market_context: dict[str, Any],
        candidates: list[dict[str, Any]],
        detailed: bool = False,
    ) -> list[str]:
        lines = self._market_lines(market_context, detailed=detailed)
        if candidates:
            lines.append("- 基于全市场基金扫描与当前组合结构，优先关注以下补仓候选：")
            lines.extend(self._candidate_lines(candidates, detailed=detailed))
        else:
            lines.append("- 当前未从全市场扫描中筛出更优候选，可先继续跟踪现有仓位和市场节奏。")
        return lines

    def _source_summary_lines(
        self,
        source_updates: list[dict[str, Any]],
        market_context: dict[str, Any],
        holdings_summary: dict[str, Any],
        candidates: list[dict[str, Any]],
        weekly: bool,
    ) -> list[str]:
        events = self._flatten_source_events(source_updates)
        if not events:
            return ["本周可用的高价值资讯较少，当前更适合继续按定投纪律执行，并等待下一批更明确的宏观信号。"]

        domestic_events = self._select_events_for_section(events, "国内宏观", limit=3)
        overseas_events = self._select_events_for_section(events, "海外宏观", limit=3)
        commodity_events = self._select_events_for_section(events, "商品与大类资产", limit=3)

        sections = [
            ("国内宏观", self._build_domestic_macro_paragraph(domestic_events)),
            ("海外宏观", self._build_overseas_macro_paragraph(overseas_events, market_context)),
            ("商品与大类资产", self._build_commodity_paragraph(commodity_events, market_context)),
            (
                "对你当前配置的参考",
                self._build_allocation_reference_paragraph(
                    holdings_summary=holdings_summary,
                    candidates=candidates,
                    domestic_events=domestic_events,
                    overseas_events=overseas_events,
                    commodity_events=commodity_events,
                    weekly=weekly,
                ),
            ),
        ]

        lines: list[str] = []
        for index, (title, paragraph) in enumerate(sections):
            lines.append(title)
            lines.append(paragraph)
            if index < len(sections) - 1:
                lines.append("")
        return lines

    def _flatten_source_events(self, source_updates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for group in source_updates:
            category = str(group.get("category", "未分类")).strip() or "未分类"
            source_name = str(group.get("name", "")).strip() or "未知来源"
            for item in group.get("items", []):
                title = self._clean_source_title(str(item.get("title", "")).strip())
                link = str(item.get("link", "")).strip()
                if not title or self._is_low_signal_title(title, source_name):
                    continue
                published_at = str(item.get("published_at", "")).strip()
                if not published_at:
                    inferred = self._infer_datetime_from_text_or_url(title, link)
                    published_at = inferred.isoformat() if inferred else ""
                dedupe_key = f"{category}|{source_name}|{title}".lower()
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                events.append(
                    {
                        "category": category,
                        "source_name": source_name,
                        "title": title,
                        "link": link,
                        "published_at": published_at,
                    }
                )
        return events

    def _select_events_for_section(
        self,
        events: list[dict[str, Any]],
        section: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        scored: list[dict[str, Any]] = []
        for event in events:
            score = self._score_event_for_section(event, section)
            if score < 4:
                continue
            scored.append({**event, "score": score})

        scored.sort(
            key=lambda item: (
                item["score"],
                item.get("published_at", ""),
            ),
            reverse=True,
        )

        selected: list[dict[str, Any]] = []
        used_sources: set[str] = set()
        for event in scored:
            source_name = event["source_name"]
            if len(selected) < max(1, limit - 1) and source_name in used_sources:
                continue
            selected.append(event)
            used_sources.add(source_name)
            if len(selected) >= limit:
                break
        return selected

    def _score_event_for_section(self, event: dict[str, Any], section: str) -> int:
        title = event["title"]
        title_lower = title.lower()
        category = event["category"]
        source_name = event["source_name"]

        section_rules: dict[str, dict[str, Any]] = {
            "国内宏观": {
                "categories": {"宏观政策", "交易所与披露", "指数与风格"},
                "source_weights": {
                    "中国人民银行": 5,
                    "国家发展和改革委员会": 4,
                    "上海证券交易所": 3,
                    "深圳证券交易所": 3,
                    "中证指数有限公司": 3,
                    "国证指数官网": 2,
                    "财联社": 1,
                },
                "keywords": (
                    "潘功胜", "记者会", "金融市场运行情况", "货币政策", "民营企业", "再融资",
                    "样本", "指数", "金融", "流动性", "债", "融资", "稳增长", "政策", "REITs",
                ),
                "exclude": (
                    "习近平", "总书记", "党建", "学习教育", "开幕", "致以节日祝福", "会见", "决定", "查看更多",
                ),
            },
            "海外宏观": {
                "categories": {"海外宏观", "媒体快讯"},
                "source_weights": {
                    "美国劳工统计局": 6,
                    "美联储": 5,
                    "财联社": 2,
                    "金十数据": 2,
                },
                "keywords": (
                    "美联储", "federal reserve", "联储", "利率", "降息", "加息", "非农", "就业", "失业", "薪资",
                    "cpi", "pce", "inflation", "美股", "欧央行", "国际油价", "美元",
                ),
                "exclude": (
                    "enforcement action", "former employee", "tokenized securities", "开放平台", "举报专区",
                ),
            },
            "商品与大类资产": {
                "categories": {"商品与大类资产", "行业协会", "媒体快讯", "宏观政策"},
                "source_weights": {
                    "世界黄金协会": 4,
                    "芝加哥商品交易所": 4,
                    "伦敦金银市场协会": 4,
                    "上海有色网": 4,
                    "伦敦金属交易所": 4,
                    "上海期货交易所": 4,
                    "国家发展和改革委员会": 2,
                    "财联社": 2,
                    "金十数据": 2,
                },
                "keywords": (
                    "黄金", "贵金属", "金价", "白银", "油价", "原油", "成品油", "铜", "铝", "有色",
                    "期货", "comex", "lbma", "lme", "资源", "石油", "天然气",
                ),
                "exclude": (
                    "industry standards", "news & events", "insights", "goldhub", "our team", "board",
                    "sub-committees", "solutions home", "market data", "tradinghero", "开放平台", "举报专区",
                ),
            },
        }

        rules = section_rules[section]
        score = 0
        if category in rules["categories"]:
            score += 3
        score += int(rules["source_weights"].get(source_name, 0))
        score += sum(2 for keyword in rules["keywords"] if keyword.lower() in title_lower)
        score -= sum(4 for keyword in rules["exclude"] if keyword.lower() in title_lower)
        if event.get("published_at"):
            score += 1
        if title.count(" ") > 10 and all(ord(ch) < 128 for ch in title):
            score -= 1
        return score

    def _build_domestic_macro_paragraph(self, events: list[dict[str, Any]]) -> str:
        if not events:
            return (
                "本周国内官方高价值增量信息不算密集，但政策主线仍围绕稳增长、稳定融资环境和资本市场制度优化展开，"
                "因此对 A 股宽基、红利和债券资产的配置逻辑并没有被削弱。"
            )

        mentions = self._join_event_mentions(events)
        fragments: list[str] = [
            f"本周国内最值得跟踪的政策线索主要来自 {mentions}。"
        ]

        titles = " ".join(event["title"] for event in events)
        if any(keyword in titles for keyword in ("金融市场运行情况", "人民银行", "潘功胜", "货币政策")):
            fragments.append("从这些信息看，监管仍然重视融资与流动性传导是否顺畅，政策语气偏向稳预期和支持实体经济。")
        if any(keyword in titles for keyword in ("民营企业", "再融资", "REITs", "样本")):
            fragments.append("资本市场和产业政策层面的信息也偏积极，说明 A 股内部仍有结构性配置机会，而不是只能把注意力放在海外市场。")

        return "".join(fragments)

    def _build_overseas_macro_paragraph(
        self,
        events: list[dict[str, Any]],
        market_context: dict[str, Any],
    ) -> str:
        official_events = [
            event for event in events if event["source_name"] in {"美联储", "美国劳工统计局"}
        ]
        media_events = [
            event for event in events if event["source_name"] in {"财联社", "金十数据"}
        ]

        fragments: list[str] = []
        if official_events:
            fragments.append(
                f"海外官方口径里，本周抓到的更新主要包括 {self._join_event_mentions(official_events[:2])}。"
            )
        else:
            fragments.append(
                "海外官方源里，本周没有抓到新的议息决议、非农或 CPI 这类会直接改写利率路径的重磅数据，"
                "市场仍处于等待下一轮宏观确认的阶段。"
            )

        if media_events:
            fragments.append(
                f"从市场端的资讯看，交易焦点更多落在 {self._join_event_mentions(media_events[:2])} 这类线索上，"
                "说明海外风险偏好仍在利率预期、能源价格和科技资本开支之间来回切换。"
            )

        indexes = market_context.get("indexes", [])
        if indexes:
            positive = sum(1 for item in indexes if float(item.get("pct_change", 0.0)) > 0)
            if positive >= len(indexes) / 2:
                fragments.append("至少从指数表现看，风险偏好暂时没有明显失速，但方向仍不够稳定。")
            else:
                fragments.append("指数层面的分化提醒你，不要把“海外市场还在涨”简单理解成可以继续无脑加码。")

        return "".join(fragments)

    def _build_commodity_paragraph(
        self,
        events: list[dict[str, Any]],
        market_context: dict[str, Any],
    ) -> str:
        fragments: list[str] = []
        if events:
            fragments.append(
                f"商品和大类资产这边，本周比较有代表性的线索是 {self._join_event_mentions(events)}。"
            )
        else:
            fragments.append(
                "本周商品官方源里没有抓到特别集中的重磅公告，但能源、黄金和资源品仍然是资产轮动里最值得盯的方向。"
            )

        theme_funds = market_context.get("theme_funds", [])
        strong_themes = [
            item["fund_name"]
            for item in theme_funds
            if any(keyword in item["fund_name"] for keyword in ("油气", "黄金", "资源", "有色"))
        ][:2]
        if strong_themes:
            fragments.append(
                "与此同时，近期强势基金方向里已经出现 "
                + "、".join(strong_themes)
                + "，说明资源和商品链条并不是边角主题，而是市场真实在交易的主线之一。"
            )

        fragments.append("这也是为什么在你的组合里，黄金比继续叠加同类美股指数更像一个值得补的对冲位。")
        return "".join(fragments)

    def _build_allocation_reference_paragraph(
        self,
        holdings_summary: dict[str, Any],
        candidates: list[dict[str, Any]],
        domestic_events: list[dict[str, Any]],
        overseas_events: list[dict[str, Any]],
        commodity_events: list[dict[str, Any]],
        weekly: bool,
    ) -> str:
        distribution = holdings_summary["distribution"]
        qdii_weight = distribution.get("qdii", 0.0)
        bond_weight = distribution.get("bond", 0.0)
        gold_weight = distribution.get("gold", 0.0)
        domestic_weight = distribution.get("domestic_index", 0.0)

        fragments: list[str] = []
        if qdii_weight >= 60:
            fragments.append("你现在几乎把全部仓位压在美股 QDII 上，因此海外风险偏好一旦反复，净值波动会被明显放大。")
        else:
            fragments.append("你当前组合虽然不是纯单边押注，但海外权益仍然会决定很大一部分净值波动。")

        if domestic_events and domestic_weight < 20:
            fragments.append("本周国内政策和市场制度层面的信息并不弱，说明新增资金没有必要继续只往纳指和标普堆。")
        if commodity_events and gold_weight < 10:
            fragments.append("考虑到本周商品和避险资产仍有存在感，黄金仓位可以作为下一步扩展配置时的第二顺位。")
        if bond_weight < 20:
            fragments.append("但如果按稳健型框架排优先级，第一步仍然应该是先把债券压舱石补起来。")

        candidate_themes = [item["theme"] for item in candidates[:4]]
        if candidate_themes:
            theme_text = "、".join(candidate_themes)
            fragments.append(
                f"结合全市场基金扫描后的重点候选摘要，后续更值得跟踪的是 {theme_text} 这些方向，"
                "而不是继续增加同质化的美股指数暴露。"
            )

        if weekly and overseas_events:
            fragments.append("如果下周出现你设定的跌幅提醒，也更建议优先把补仓资金投向这些“补短板”的方向，而不是条件反射式抄底原有 QDII。")

        return "".join(fragments)

    def _join_event_mentions(self, events: list[dict[str, Any]]) -> str:
        mentions = [self._event_mention(event) for event in events if event.get("title")]
        if not mentions:
            return "暂无明确高价值事件"
        if len(mentions) == 1:
            return mentions[0]
        if len(mentions) == 2:
            return f"{mentions[0]}和{mentions[1]}"
        return "、".join(mentions[:-1]) + "以及" + mentions[-1]

    def _event_mention(self, event: dict[str, Any]) -> str:
        title = self._short_report_title(event["title"])
        date_text = self._event_date_text(event)
        if date_text:
            return f"{date_text}的“{title}”"
        return f"“{title}”"

    def _event_date_text(self, event: dict[str, Any]) -> str:
        raw = str(event.get("published_at", "")).strip()
        parsed = self._parse_datetime(raw) if raw else None
        if parsed is None:
            parsed = self._infer_datetime_from_text_or_url(event["title"], event.get("link", ""))
        if parsed is None:
            return ""
        return f"{parsed.month}月{parsed.day}日"

    def _clean_source_title(self, title: str) -> str:
        normalized = re.sub(r"\s+", " ", title).strip(" -|·")
        normalized = re.sub(r"(.{12,}?)\1+", r"\1", normalized)
        if "..." in normalized and normalized.count("...") >= 1:
            normalized = re.sub(r"\.\.\..*$", "...", normalized)
        lowered = normalized.lower()
        if (
            not normalized
            or len(normalized) < 6
            or normalized in {"首页", "更多", "登录", "注册", "下载", "联系我们"}
            or any(
                fragment in lowered
                for fragment in (
                    "english version", "market data home", "markets home", "contact", "about", "var ",
                    "{{", "}}", "goldhub", "industry standards", "solutions home",
                )
            )
        ):
            return ""
        return normalized

    def _is_low_signal_title(self, title: str, source_name: str) -> bool:
        lowered = title.lower()
        bad_fragments = (
            "习近平", "总书记", "会见", "开幕", "党建", "学习教育", "述责述廉", "致以节日祝福",
            "查看更多", "问询函", "监管措施", "our team", "board", "sub-committees", "open platform",
            "tradinghero", "举报专区", "goldhub", "industry standards", "news & events", "insights",
            "market data", "solutions home",
        )
        if any(fragment.lower() in lowered for fragment in bad_fragments):
            return True
        if source_name == "金十数据" and len(title) <= 8:
            return True
        return False

    def _short_report_title(self, title: str) -> str:
        if len(title) <= 36:
            return title
        return f"{title[:36]}..."

    def _extract_news_themes(self, titles: list[str]) -> Counter[str]:
        theme_rules = {
            "海外流动性": ("美联储", "联储", "FOMC", "降息", "利率", "就业", "非农", "CPI", "PCE", "通胀", "失业"),
            "国内稳增长": ("人民银行", "央行", "货币政策", "财政", "发改委", "PMI", "社融", "M2", "消费", "投资", "地产"),
            "黄金与大宗商品": ("黄金", "白银", "贵金属", "原油", "铜", "铝", "有色", "商品", "LBMA", "LME", "COMEX"),
            "科技成长": ("科技", "人工智能", "AI", "算力", "芯片", "半导体", "机器人", "自动化"),
            "红利低波": ("红利", "分红", "低波", "高股息", "央企"),
            "指数与ETF": ("指数", "成分股", "样本", "再平衡", "ETF", "基金"),
        }
        counter: Counter[str] = Counter()
        lowered_titles = [title.lower() for title in titles]
        for theme, keywords in theme_rules.items():
            hits = 0
            for title in lowered_titles:
                if any(keyword.lower() in title for keyword in keywords):
                    hits += 1
            if hits:
                counter[theme] = hits
        return counter

    def _build_macro_summary_line(self, theme_counter: Counter[str], market_context: dict[str, Any]) -> str:
        fragments: list[str] = []
        if theme_counter.get("海外流动性"):
            fragments.append("海外流动性预期仍在反复，利率与就业数据对美股估值影响较大")
        if theme_counter.get("国内稳增长"):
            fragments.append("国内政策线索偏向稳增长与托底，A 股宽基和红利风格的配置价值在提升")
        if theme_counter.get("黄金与大宗商品"):
            fragments.append("黄金与大宗商品主题活跃，说明市场仍保留一定避险和通胀对冲需求")

        indexes = market_context.get("indexes", [])
        if indexes:
            positive_count = sum(1 for item in indexes if float(item.get("pct_change", 0.0)) > 0)
            if positive_count >= len(indexes) / 2:
                fragments.append("主要指数表现偏强，风险偏好暂未明显走弱")
            else:
                fragments.append("主要指数分化较大，市场并未形成单边趋势")

        if not fragments:
            return "- 宏观结论：目前外部信号仍偏混合，政策、利率和风险偏好三条主线并行，适合继续以分散配置应对。"
        return "- 宏观结论：" + "；".join(fragments) + "。"

    def _build_market_summary_line(self, market_context: dict[str, Any]) -> str:
        sectors = market_context.get("sectors", [])
        top_sectors = [item["name"] for item in sectors if item.get("direction") == "top"][:3]
        bottom_sectors = [item["name"] for item in sectors if item.get("direction") == "bottom"][:2]
        theme_funds = market_context.get("theme_funds", [])
        leading_funds = [item["fund_name"] for item in theme_funds[:2]]

        fragments: list[str] = []
        if top_sectors:
            fragments.append("近期相对强势的板块集中在 " + "、".join(top_sectors))
        if bottom_sectors:
            fragments.append("走弱板块主要是 " + "、".join(bottom_sectors))
        if leading_funds:
            fragments.append("强势基金方向更多落在 " + "、".join(leading_funds))

        if not fragments:
            return ""
        return "- 市场结构：" + "；".join(fragments) + "。"

    def _build_portfolio_implication_line(
        self,
        holdings_summary: dict[str, Any],
        theme_counter: Counter[str],
        weekly: bool,
    ) -> str:
        distribution = holdings_summary["distribution"]
        qdii_weight = distribution.get("qdii", 0.0)
        bond_weight = distribution.get("bond", 0.0)
        gold_weight = distribution.get("gold", 0.0)
        domestic_weight = distribution.get("domestic_index", 0.0)

        if qdii_weight >= 60:
            recommendation = "你当前组合仍以 QDII 美股指数为主，和“海外流动性”主线高度绑定，短期更应该做分散而不是继续集中。"
        elif domestic_weight < 20:
            recommendation = "当前 A 股底仓偏轻，如果国内稳增长信号继续增强，可逐步提高宽基或红利仓位。"
        else:
            recommendation = "当前组合分散度已有一定基础，接下来更重要的是控制加仓节奏而不是频繁换仓。"

        supplements: list[str] = []
        if bond_weight < 20:
            supplements.append("债券仓位仍建议优先补足")
        if gold_weight < 10 and theme_counter.get("黄金与大宗商品"):
            supplements.append("黄金可作为第二层对冲")
        if weekly and theme_counter.get("国内稳增长") and domestic_weight < 20:
            supplements.append("周度视角下可重点观察 A 股宽基与红利方向")

        if supplements:
            recommendation = recommendation + " 同时，" + "、".join(supplements) + "。"

        return "- 对当前持仓的含义：" + recommendation

    def _display_category_name(self, category: str) -> str:
        mapping = {
            "qdii": "海外权益/QDII",
            "bond": "债券",
            "gold": "黄金",
            "domestic_index": "A股宽基",
            "money": "货币",
            "fof": "FOF",
            "active_equity": "主动权益",
            "other": "其他",
        }
        return mapping.get(category, category)

    def _briefing_market_context(self, market_context: dict[str, Any]) -> dict[str, Any]:
        return {
            "indexes": market_context.get("indexes", []),
            "sectors": market_context.get("sectors", []),
            "theme_funds": market_context.get("theme_funds", []),
        }

    def _candidate_lines(self, candidates: list[dict[str, Any]], detailed: bool = False) -> list[str]:
        if not candidates:
            return ["- 当前未生成候选基金，建议先补充更多研究信息。"]

        lines: list[str] = []
        for item in candidates:
            lines.append(f"- {item['theme']}：{item['reason']}")
            for fund in item["funds"]:
                perf_bits = []
                if fund.get("one_month") is not None:
                    perf_bits.append(f"近1月 {fund['one_month']:.2f}%")
                if fund.get("one_year") is not None:
                    perf_bits.append(f"近1年 {fund['one_year']:.2f}%")
                perf_text = "，".join(perf_bits) if perf_bits else "暂无近期收益数据"
                line = (
                    f"  - {fund['fund_code']} {fund['fund_name']} | {fund['category']} | "
                    f"{perf_text} | 费率 {fund['fee'] or '未知'}"
                )
                lines.append(line)
                if detailed:
                    lines.append("    - 说明：这是全市场基金扫描后压缩出的重点候选摘要，建议结合公告、指数样本和基金规模再次确认。")
        return lines

    def _trend_alignment_lines(self, holdings_summary: dict[str, Any]) -> list[str]:
        distribution = holdings_summary["distribution"]
        lines: list[str] = []
        if distribution.get("qdii", 0.0) >= 60:
            lines.append("- 当前组合主要押注美股大盘指数，更像单一风格配置，不符合稳健型“多资产分散”要求。")
        if distribution.get("bond", 0.0) < 20:
            lines.append("- 债券资产不足，意味着组合对利率、权益波动和海外市场回撤更敏感。")
        if distribution.get("gold", 0.0) < 10:
            lines.append("- 缺少黄金类资产，对通胀预期、美元波动和避险情绪的对冲较弱。")
        if distribution.get("domestic_index", 0.0) < 20:
            lines.append("- 缺少 A 股宽基/红利底仓，不利于在中美市场轮动时维持配置弹性。")
        return lines or ["- 当前配置与稳健型目标大体一致，继续按纪律定投并做小幅再平衡即可。"]
