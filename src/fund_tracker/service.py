from __future__ import annotations

import csv
import json
import re
import smtplib
import sqlite3
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from src.fund_tracker.codex_briefing import CodexMonthlyBriefingRunner
from src.fund_tracker.config import TrackerConfig
from src.fund_tracker.external_research import ExternalResearchEngine
from src.fund_tracker.parser import CommandParseError, ParsedCommand, parse_command
from src.fund_tracker.pricing import EastMoneyPriceProvider, FundPricePayload, PricePoint


WEEKDAY_TO_RULE = {
    0: "weekly:MON",
    1: "weekly:TUE",
    2: "weekly:WED",
    3: "weekly:THU",
    4: "weekly:FRI",
    5: "weekly:SAT",
    6: "weekly:SUN",
}

PLAN_ACTION_LABELS = {
    "buy": "买入",
    "sell": "卖出",
    "create_dca": "新增定投",
    "update_dca": "修改定投",
    "pause_dca": "暂停定投",
    "resume_dca": "恢复定投",
    "cancel_dca": "取消定投",
}

PLAN_DCA_ACTIONS = {"create_dca", "update_dca", "pause_dca", "resume_dca", "cancel_dca"}


@dataclass
class CommandResult:
    message: str
    payload: dict[str, Any]


class FundTrackerService:
    def __init__(
        self,
        conn: sqlite3.Connection,
        config: TrackerConfig,
        price_provider: EastMoneyPriceProvider | None = None,
    ) -> None:
        self.conn = conn
        self.config = config
        self.price_provider = price_provider or EastMoneyPriceProvider()

    def import_initial_holdings(self, csv_path: Path) -> CommandResult:
        inserted = 0
        with open(csv_path, "r", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                fund_code = row["fund_code"].strip()
                fund_name = row.get("fund_name", "").strip() or fund_code
                shares = float(row["shares"])
                trade_date = row.get("trade_date", "") or date.today().isoformat()
                if row.get("cost_nav"):
                    nav = float(row["cost_nav"])
                    amount = float(row.get("amount") or nav * shares)
                elif row.get("amount"):
                    amount = float(row["amount"])
                    nav = amount / shares
                else:
                    raise ValueError(f"初始持仓缺少 cost_nav 或 amount：{fund_code}")

                drop_threshold = float(
                    row.get("drop_threshold_pct") or self.config.default_drop_threshold_pct
                )
                note = row.get("note", "").strip()

                self._upsert_fund(fund_code, fund_name, drop_threshold)
                self._insert_transaction(
                    fund_code=fund_code,
                    trade_date=trade_date,
                    trade_type="initial",
                    amount=amount,
                    nav=nav,
                    shares=shares,
                    fee=0,
                    source="initial",
                    status="posted",
                    note=note or "初始持仓导入",
                    raw_text="initial import",
                    plan_id=None,
                    order_date=trade_date,
                    confirm_nav_date=trade_date,
                    effective_from_date=trade_date,
                )
                inserted += 1

        return CommandResult(
            message=f"已导入 {inserted} 条初始持仓。",
            payload={"inserted_count": inserted},
        )

    def apply_text_command(
        self,
        text: str,
        trade_date: date | None = None,
        order_at: datetime | None = None,
    ) -> CommandResult:
        parsed = parse_command(text)
        explicit_trade_date = parsed.payload.get("explicit_trade_date")
        explicit_date = date.fromisoformat(explicit_trade_date) if explicit_trade_date else None
        command_date = trade_date or explicit_date or (order_at.date() if order_at else date.today())
        command_at = order_at or self._order_datetime_for_command(command_date)

        if parsed.action == "trade":
            return self._handle_trade(parsed, text, command_date, command_at)
        if parsed.action == "create_dca":
            return self._handle_create_dca(parsed, command_date)
        if parsed.action == "pause_dca":
            return self._set_dca_enabled(parsed.payload["identifier"], False)
        if parsed.action == "resume_dca":
            return self._set_dca_enabled(parsed.payload["identifier"], True)
        if parsed.action == "cancel_dca":
            return self._cancel_dca(parsed.payload["identifier"])
        if parsed.action in {"view_holdings", "view_performance", "analyze_portfolio"}:
            snapshot = self.build_portfolio_snapshot()
            if parsed.action == "analyze_portfolio":
                report = self._build_local_analysis(snapshot)
                self._save_analysis_report(
                    report_type="manual",
                    input_snapshot=snapshot,
                    skill_name="local-baseline",
                    report_body=report,
                )
                return CommandResult(message="已生成持仓分析。", payload={"snapshot": snapshot, "report": report})
            return CommandResult(message="已生成组合快照。", payload={"snapshot": snapshot})

        raise CommandParseError(f"暂不支持的指令：{parsed.action}")

    def run_daily(self, run_date: date | None = None) -> CommandResult:
        target_date = run_date or date.today()
        due_dates = self._materialize_dca_transactions(target_date)
        price_updates = self.refresh_prices()
        alerts = self._check_alerts()
        snapshot = self.build_portfolio_snapshot()
        snapshot_path = self.save_snapshot(snapshot)
        external_reports: list[dict[str, Any]] = []
        try:
            if self._is_month_end(target_date):
                monthly_report = self.generate_external_analysis_report(
                    mode="monthly",
                    report_date=target_date,
                    snapshot=snapshot,
                )
                external_reports.append(
                    {
                        "report_type": "external_monthly",
                        "message": monthly_report.message,
                    }
                )
        except Exception as exc:
            external_reports.append({"report_type": "external_error", "message": str(exc)})

        try:
            daily_opportunity = self.generate_daily_opportunity_report(
                report_date=target_date,
                snapshot=snapshot,
                notify=True,
            )
            external_reports.append(
                {
                    "report_type": "external_daily_opportunity",
                    "message": daily_opportunity.message,
                    "recommendation_level": daily_opportunity.payload.get("recommendation_level"),
                    "should_alert": daily_opportunity.payload.get("should_alert"),
                }
            )
        except Exception as exc:
            external_reports.append(
                {"report_type": "external_daily_opportunity_error", "message": str(exc)}
            )

        return CommandResult(
            message="每日任务执行完成。",
            payload={
                "run_date": target_date.isoformat(),
                "dca_dates": due_dates,
                "price_updates": price_updates,
                "alerts": alerts,
                "snapshot_path": str(snapshot_path),
                "external_reports": external_reports,
            },
        )

    def generate_external_analysis_report(
        self,
        mode: str = "monthly",
        report_date: date | None = None,
        snapshot: dict[str, Any] | None = None,
        available_cash: float | None = None,
    ) -> CommandResult:
        if mode != "monthly":
            raise ValueError("增强周报已关闭，当前仅支持生成增强月报。")
        normalized_mode = "monthly"
        effective_report_date = report_date or date.today()
        current_snapshot = snapshot or self.build_portfolio_snapshot(as_of=effective_report_date)
        engine = self._external_research_engine()
        material_packet = engine.build_monthly_briefing_material_packet(
            snapshot=current_snapshot,
            available_cash=available_cash,
            risk_profile="稳健",
            as_of=effective_report_date,
        )
        report_payload = self._codex_monthly_briefing_runner().generate_monthly_report(material_packet)
        report_body = str(report_payload.get("report_body", "")).strip()
        execution_plan = self._normalize_execution_plan(report_payload.get("execution_plan"))
        report_type = f"external_{normalized_mode}"
        skill_name = "codex+fund-portfolio-advisor+fund-monthly-briefing"
        self._save_analysis_report(
            report_type=report_type,
            input_snapshot={
                "portfolio_snapshot": current_snapshot,
                "available_cash": round(float(available_cash), 2) if available_cash else None,
                "execution_plan": execution_plan,
                "execution_applied": None,
            },
            skill_name=skill_name,
            report_body=report_body,
            report_date=effective_report_date,
        )
        return CommandResult(
            message="已生成每月增强投资建议。",
            payload={
                "snapshot": current_snapshot,
                "report": report_body,
                "report_type": report_type,
                "execution_plan": execution_plan,
            },
        )

    def generate_cash_deployment_plan(
        self,
        available_cash: float,
        report_date: date | None = None,
        snapshot: dict[str, Any] | None = None,
    ) -> CommandResult:
        if available_cash <= 0:
            raise ValueError("可支配资金必须大于 0。")

        effective_report_date = report_date or date.today()
        current_snapshot = snapshot or self.build_portfolio_snapshot(as_of=effective_report_date)
        latest_monthly_report = self._latest_analysis_report(report_type="external_monthly")
        engine = self._external_research_engine()
        material_packet = engine.build_cash_deployment_material_packet(
            snapshot=current_snapshot,
            available_cash=available_cash,
            latest_monthly_report=latest_monthly_report,
            risk_profile="稳健",
            as_of=effective_report_date,
        )
        report_body = self._codex_monthly_briefing_runner().generate_cash_deployment_plan(material_packet)
        report_type = "external_cash_plan"
        skill_name = "codex+fund-portfolio-advisor+fund-cash-deployment-plan"
        self._save_analysis_report(
            report_type=report_type,
            input_snapshot={
                "available_cash": round(float(available_cash), 2),
                "portfolio_snapshot": current_snapshot,
                "latest_monthly_report_id": latest_monthly_report.get("id") if latest_monthly_report else None,
            },
            skill_name=skill_name,
            report_body=report_body,
            report_date=effective_report_date,
        )
        return CommandResult(
            message=f"已生成 {available_cash:.2f} 元可支配资金的调整方案。",
            payload={
                "snapshot": current_snapshot,
                "available_cash": round(float(available_cash), 2),
                "report": report_body,
                "report_type": report_type,
            },
        )

    def generate_daily_opportunity_report(
        self,
        report_date: date | None = None,
        snapshot: dict[str, Any] | None = None,
        available_cash: float | None = None,
        notify: bool = False,
    ) -> CommandResult:
        effective_report_date = report_date or date.today()
        current_snapshot = snapshot or self.build_portfolio_snapshot(as_of=effective_report_date)
        latest_monthly_report = self._latest_analysis_report(report_type="external_monthly")
        engine = self._external_research_engine()
        material_packet = engine.build_daily_opportunity_material_packet(
            snapshot=current_snapshot,
            latest_monthly_report=latest_monthly_report,
            available_cash=available_cash,
            risk_profile="稳健",
            as_of=effective_report_date,
        )
        report_payload = self._codex_monthly_briefing_runner().generate_daily_opportunity_report(
            material_packet
        )
        report_body = str(report_payload.get("report_body", "")).strip()
        recommendation_level = str(report_payload.get("recommendation_level") or "watch").strip() or "watch"
        should_alert = bool(report_payload.get("should_alert"))
        summary = str(report_payload.get("summary") or "").strip()
        no_action_reason = report_payload.get("no_action_reason")
        if no_action_reason not in (None, ""):
            no_action_reason = str(no_action_reason).strip()
        else:
            no_action_reason = None
        expires_at = report_payload.get("expires_at")
        if expires_at not in (None, ""):
            expires_at = str(expires_at).strip()
        else:
            expires_at = None
        opportunities = report_payload.get("opportunities")
        if not isinstance(opportunities, list):
            opportunities = []
        opportunities = [item for item in opportunities if isinstance(item, dict)]

        notification_results: list[dict[str, Any]] = []
        if notify and should_alert and opportunities:
            notification_results = self._send_daily_opportunity_notifications(
                report_date=effective_report_date,
                summary=summary or report_body,
                opportunities=opportunities,
            )

        report_type = "external_daily_opportunity"
        skill_name = "codex+fund-portfolio-advisor+fund-daily-opportunity-monitor"
        self._save_analysis_report(
            report_type=report_type,
            input_snapshot={
                "portfolio_snapshot": current_snapshot,
                "available_cash": round(float(available_cash), 2)
                if available_cash is not None and available_cash > 0
                else None,
                "latest_monthly_report_id": latest_monthly_report.get("id") if latest_monthly_report else None,
                "candidate_universe_scope": material_packet.get("candidate_universe_scope"),
                "priority_industry_watchlist": material_packet.get("priority_industry_watchlist"),
                "priority_industry_watch_snapshot": material_packet.get("priority_industry_watch_snapshot"),
                "recommendation_level": recommendation_level,
                "should_alert": should_alert,
                "summary": summary,
                "no_action_reason": no_action_reason,
                "opportunities": opportunities,
                "expires_at": expires_at,
                "notification_results": notification_results,
            },
            skill_name=skill_name,
            report_body=report_body,
            report_date=effective_report_date,
        )
        return CommandResult(
            message="已生成今日强机会监测结果。",
            payload={
                "snapshot": current_snapshot,
                "priority_industry_watch_snapshot": material_packet.get("priority_industry_watch_snapshot"),
                "report": report_body,
                "report_type": report_type,
                "recommendation_level": recommendation_level,
                "should_alert": should_alert,
                "summary": summary,
                "no_action_reason": no_action_reason,
                "opportunities": opportunities,
                "expires_at": expires_at,
                "notification_results": notification_results,
            },
        )

    def apply_analysis_report_execution_plan(
        self,
        report_id: int,
        actions: list[dict[str, Any]],
        trade_date: date | None = None,
    ) -> CommandResult:
        report_row = self.conn.execute(
            """
            SELECT id, report_type, input_snapshot, report_body, created_at
            FROM analysis_reports
            WHERE id = ?
            """,
            (report_id,),
        ).fetchone()
        if report_row is None:
            raise ValueError(f"投资建议 #{report_id} 不存在。")

        snapshot_payload = self._load_json_object(report_row["input_snapshot"])
        execution_applied = snapshot_payload.get("execution_applied")
        if isinstance(execution_applied, dict) and execution_applied.get("executed_at"):
            raise ValueError("这份资金调配方案已经执行过，当前版本不支持重复执行。")

        normalized_actions = self._normalize_execution_plan(actions)
        if not normalized_actions:
            raise ValueError("没有可执行的方案项。")

        effective_trade_date = trade_date or date.today()
        same_day_context = self.build_portfolio_snapshot(as_of=effective_trade_date).get(
            "same_day_execution_context",
            {},
        )
        reserved_amount_by_code = {
            str(item.get("fund_code")): float(item.get("today_due_dca_amount", 0.0) or 0.0)
            for item in same_day_context.get("tracked_fund_limits", [])
            if item.get("fund_code")
        }
        purchase_limit_by_code = {
            str(item.get("fund_code")): (
                round(float(item["daily_purchase_limit_amount"]), 2)
                if item.get("daily_purchase_limit_amount") is not None
                else None
            )
            for item in same_day_context.get("tracked_fund_limits", [])
            if item.get("fund_code")
        }

        engine = self._external_research_engine()
        resolved_actions: list[dict[str, Any]] = []
        for index, action in enumerate(normalized_actions, start=1):
            resolved_action = dict(action)
            identifier = (
                str(action.get("fund_code") or "").strip()
                or str(action.get("fund_name") or "").strip()
            )
            if not identifier:
                raise ValueError(f"第 {index} 行缺少基金代码或基金名称。")

            fund = self._resolve_fund(identifier)
            resolved_action["fund_code"] = fund["fund_code"]
            resolved_action["fund_name"] = fund["fund_name"]

            amount = resolved_action.get("amount")
            if resolved_action["action_type"] in {"buy", "sell", "create_dca", "update_dca"}:
                if amount is None or float(amount) <= 0:
                    raise ValueError(f"第 {index} 行 {fund['fund_name']} 缺少有效金额。")
                rounded_amount = round(float(amount), 2)
                if abs((rounded_amount / 10.0) - round(rounded_amount / 10.0)) > 1e-6:
                    raise ValueError(f"第 {index} 行 {fund['fund_name']} 的金额必须是 10 元整数倍。")
                resolved_action["amount"] = rounded_amount

            if resolved_action["action_type"] in {"create_dca", "update_dca"}:
                frequency = str(resolved_action.get("frequency") or "").strip().lower()
                run_rule = self._normalize_dca_run_rule(
                    frequency=frequency,
                    run_rule=resolved_action.get("run_rule"),
                )
                resolved_action["frequency"] = frequency
                resolved_action["run_rule"] = run_rule

            if resolved_action["action_type"] == "buy":
                fund_code = str(resolved_action["fund_code"])
                if fund_code not in purchase_limit_by_code:
                    constraint = engine.lookup_fund_trade_constraint(
                        fund_code,
                        str(resolved_action["fund_name"]),
                    )
                    purchase_limit_by_code[fund_code] = (
                        round(float(constraint["daily_purchase_limit_amount"]), 2)
                        if constraint.get("daily_purchase_limit_amount") is not None
                        else None
                    )
                resolved_action["daily_purchase_limit_amount"] = purchase_limit_by_code[fund_code]

            resolved_actions.append(resolved_action)

        planned_purchase_amount_by_code: dict[str, float] = defaultdict(float)
        for index, action in enumerate(resolved_actions, start=1):
            if action["action_type"] != "buy":
                continue
            fund_code = str(action["fund_code"])
            purchase_limit = action.get("daily_purchase_limit_amount")
            if purchase_limit is None:
                continue
            occupied = float(reserved_amount_by_code.get(fund_code, 0.0))
            occupied += float(planned_purchase_amount_by_code.get(fund_code, 0.0))
            next_total = occupied + float(action["amount"])
            if next_total > float(purchase_limit) + 1e-6:
                raise ValueError(
                    f"第 {index} 行 {action['fund_name']}（{fund_code}）超出今日限额："
                    f"已占用 {occupied:.2f} 元，本次再买 {float(action['amount']):.2f} 元，"
                    f"但单日上限仅 {float(purchase_limit):.2f} 元。"
                )
            planned_purchase_amount_by_code[fund_code] += float(action["amount"])

        execution_results: list[dict[str, Any]] = []
        now = _now_iso()
        with self.conn:
            for action in resolved_actions:
                if action["action_type"] in {"buy", "sell"}:
                    result = self._execute_trade_plan_action(
                        action=action,
                        command_date=effective_trade_date,
                        report_id=report_id,
                    )
                else:
                    result = self._execute_dca_plan_action(
                        action=action,
                        command_date=effective_trade_date,
                        now=now,
                    )
                execution_results.append(result)

            snapshot_payload["execution_plan"] = resolved_actions
            snapshot_payload["execution_applied"] = {
                "executed_at": now,
                "trade_date": effective_trade_date.isoformat(),
                "results": execution_results,
            }
            self.conn.execute(
                """
                UPDATE analysis_reports
                SET input_snapshot = ?
                WHERE id = ?
                """,
                (json.dumps(snapshot_payload, ensure_ascii=False), report_id),
            )

        return CommandResult(
            message=f"已执行 {len(execution_results)} 条资金调配方案。",
            payload={
                "report_id": report_id,
                "trade_date": effective_trade_date.isoformat(),
                "results": execution_results,
            },
        )

    def _normalize_execution_plan(self, raw_plan: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_plan, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in raw_plan:
            if not isinstance(item, dict):
                continue
            action_type = str(item.get("action_type") or "").strip()
            if action_type not in PLAN_ACTION_LABELS:
                continue

            fund_code = str(item.get("fund_code") or "").strip()
            fund_name = str(item.get("fund_name") or "").strip()
            if not fund_code and not fund_name:
                continue

            amount_value = item.get("amount")
            amount: float | None = None
            if amount_value not in (None, ""):
                try:
                    amount = round(float(amount_value), 2)
                except (TypeError, ValueError):
                    amount = None

            frequency_raw = str(item.get("frequency") or "").strip().lower()
            frequency = frequency_raw if frequency_raw in {"daily", "weekly"} else None
            run_rule = str(item.get("run_rule") or "").strip() or None
            if frequency == "daily":
                run_rule = "daily"

            default_sign = "-" if action_type in {"sell", "pause_dca", "cancel_dca"} else "+"
            sign = str(item.get("sign") or default_sign).strip()
            if sign not in {"+", "-"}:
                sign = default_sign

            normalized.append(
                {
                    "action_type": action_type,
                    "sign": sign,
                    "action_label": str(item.get("action_label") or PLAN_ACTION_LABELS[action_type]).strip(),
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "amount": amount,
                    "frequency": frequency,
                    "run_rule": run_rule,
                    "note": str(item.get("note") or "").strip() or None,
                }
            )
        return normalized

    def _normalize_dca_run_rule(self, frequency: str, run_rule: Any) -> str:
        if frequency == "daily":
            return "daily"
        if frequency != "weekly":
            raise ValueError("定投频率仅支持 daily 或 weekly。")

        normalized_rule = str(run_rule or "").strip().upper()
        if normalized_rule.startswith("WEEKLY:"):
            normalized_rule = f"weekly:{normalized_rule.split(':', 1)[1]}"
        if normalized_rule.startswith("weekly:"):
            suffix = normalized_rule.split(":", 1)[1].upper()
        else:
            suffix = normalized_rule
        if suffix not in {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}:
            raise ValueError("每周定投需要明确到星期几。")
        return f"weekly:{suffix}"

    def _load_json_object(self, raw_value: Any) -> dict[str, Any]:
        if isinstance(raw_value, dict):
            return dict(raw_value)
        if not raw_value:
            return {}
        if not isinstance(raw_value, str):
            return {}
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _execute_trade_plan_action(
        self,
        action: dict[str, Any],
        command_date: date,
        report_id: int,
    ) -> dict[str, Any]:
        fund_code = str(action["fund_code"])
        payload = self.price_provider.fetch_payload(fund_code)
        order_at = self._order_datetime_for_command(command_date)
        settled = self._resolve_transaction_settlement_dates(
            payload.history,
            order_at=order_at,
            fund_code=fund_code,
            fund_name=payload.fund_name,
        )
        if settled is None:
            raise ValueError(f"基金 {fund_code} 在 {command_date.isoformat()} 及之后没有净值数据。")
        confirm_point, effective_from_date, used_fallback_price = settled

        self._upsert_fund(
            payload.fund_code,
            payload.fund_name,
            self.config.default_drop_threshold_pct,
        )
        self.conn.execute(
            """
            INSERT INTO daily_prices (
                fund_code, price_date, nav, pct_change_vs_prev, source_name, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_code, price_date) DO UPDATE SET
                nav = excluded.nav,
                pct_change_vs_prev = excluded.pct_change_vs_prev,
                source_name = excluded.source_name,
                fetched_at = excluded.fetched_at
            """,
            (
                payload.fund_code,
                confirm_point.price_date.isoformat(),
                round(confirm_point.nav, 4),
                confirm_point.pct_change_vs_prev,
                payload.source_name,
                _now_iso(),
            ),
        )

        amount = round(float(action["amount"]), 2)
        shares = round(amount / confirm_point.nav, 4)
        trade_type = "buy" if action["action_type"] == "buy" else "sell"
        fee = self._compute_purchase_fee_amount(fund_code, payload.fund_name, amount) if trade_type == "buy" else 0.0
        if trade_type == "sell":
            current_shares = self._current_shares_from_transactions(fund_code)
            if shares > current_shares + 1e-6:
                raise ValueError(
                    f"卖出金额对应份额 {shares:.4f} 超过当前可用份额 {current_shares:.4f}：{fund_code}"
                )

        note = action.get("note") or f"来自增强月报 #{report_id}"
        raw_text = (
            f"{PLAN_ACTION_LABELS[action['action_type']]} "
            f"{amount:.2f} {action['fund_name']}（{fund_code}）"
        )
        self._insert_transaction(
            fund_code=fund_code,
            trade_date=confirm_point.price_date.isoformat(),
            trade_type=trade_type,
            amount=amount,
            nav=confirm_point.nav,
            shares=shares,
            fee=fee,
            source="analysis_report",
            status="posted",
            note=note,
            raw_text=raw_text,
            plan_id=None,
            order_date=command_date.isoformat(),
            order_at=order_at.isoformat(timespec="seconds"),
            confirm_nav_date=confirm_point.price_date.isoformat(),
            effective_from_date=effective_from_date,
        )
        return {
            "action_type": action["action_type"],
            "fund_code": fund_code,
            "fund_name": action["fund_name"],
            "amount": amount,
            "trade_date": confirm_point.price_date.isoformat(),
            "order_date": command_date.isoformat(),
            "order_at": order_at.isoformat(timespec="seconds"),
            "confirm_nav_date": confirm_point.price_date.isoformat(),
            "effective_from_date": effective_from_date,
            "nav": round(confirm_point.nav, 4),
            "shares": shares,
            "fee": round(fee, 2),
            "used_fallback_price": used_fallback_price,
        }

    def _execute_dca_plan_action(
        self,
        action: dict[str, Any],
        command_date: date,
        now: str,
    ) -> dict[str, Any]:
        fund_code = str(action["fund_code"])
        fund_name = str(action["fund_name"])
        active_plan = self._find_active_dca_plan(fund_code)
        latest_plan = active_plan or self._find_latest_dca_plan(fund_code)
        note = action.get("note")

        if action["action_type"] in {"create_dca", "update_dca"}:
            amount = round(float(action["amount"]), 2)
            frequency = str(action["frequency"])
            run_rule = str(action["run_rule"])
            if active_plan:
                self.conn.execute(
                    """
                    UPDATE dca_plans
                    SET amount = ?, frequency = ?, run_rule = ?, enabled = 1,
                        end_date = NULL, note = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (amount, frequency, run_rule, note, now, active_plan["id"]),
                )
                plan_id = int(active_plan["id"])
            else:
                cursor = self.conn.execute(
                    """
                    INSERT INTO dca_plans (
                        fund_code, amount, frequency, run_rule, enabled, start_date,
                        end_date, note, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, ?)
                    """,
                    (
                        fund_code,
                        amount,
                        frequency,
                        run_rule,
                        command_date.isoformat(),
                        note,
                        now,
                        now,
                    ),
                )
                plan_id = int(cursor.lastrowid)
            return {
                "action_type": action["action_type"],
                "fund_code": fund_code,
                "fund_name": fund_name,
                "amount": amount,
                "frequency": frequency,
                "run_rule": run_rule,
                "plan_id": plan_id,
            }

        if action["action_type"] == "pause_dca":
            if active_plan is None:
                raise ValueError(f"{fund_name}（{fund_code}）当前没有可暂停的定投计划。")
            self.conn.execute(
                """
                UPDATE dca_plans
                SET enabled = 0, updated_at = ?
                WHERE id = ?
                """,
                (now, active_plan["id"]),
            )
            return {
                "action_type": action["action_type"],
                "fund_code": fund_code,
                "fund_name": fund_name,
                "plan_id": int(active_plan["id"]),
            }

        if action["action_type"] == "resume_dca":
            if active_plan is not None:
                return {
                    "action_type": action["action_type"],
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "plan_id": int(active_plan["id"]),
                    "already_active": True,
                }
            if latest_plan is None:
                raise ValueError(f"{fund_name}（{fund_code}）没有历史定投计划可恢复。")
            self.conn.execute(
                """
                UPDATE dca_plans
                SET enabled = 1, end_date = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, latest_plan["id"]),
            )
            return {
                "action_type": action["action_type"],
                "fund_code": fund_code,
                "fund_name": fund_name,
                "plan_id": int(latest_plan["id"]),
            }

        if action["action_type"] == "cancel_dca":
            if active_plan is None:
                raise ValueError(f"{fund_name}（{fund_code}）当前没有可取消的定投计划。")
            self.conn.execute(
                """
                UPDATE dca_plans
                SET enabled = 0, end_date = COALESCE(end_date, ?), updated_at = ?
                WHERE id = ?
                """,
                (command_date.isoformat(), now, active_plan["id"]),
            )
            return {
                "action_type": action["action_type"],
                "fund_code": fund_code,
                "fund_name": fund_name,
                "plan_id": int(active_plan["id"]),
            }

        raise ValueError(f"暂不支持的定投动作：{action['action_type']}")

    def _find_active_dca_plan(self, fund_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM dca_plans
            WHERE fund_code = ? AND enabled = 1
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (fund_code,),
        ).fetchone()

    def _find_latest_dca_plan(self, fund_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM dca_plans
            WHERE fund_code = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (fund_code,),
        ).fetchone()

    def _current_shares_from_transactions(self, fund_code: str) -> float:
        row = self.conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN trade_type IN ('buy', 'dca', 'initial') THEN shares ELSE 0 END), 0) AS buy_shares,
                COALESCE(SUM(CASE WHEN trade_type = 'sell' THEN shares ELSE 0 END), 0) AS sell_shares
            FROM transactions
            WHERE fund_code = ? AND status = 'posted'
            """,
            (fund_code,),
        ).fetchone()
        if row is None:
            return 0.0
        return round(float(row["buy_shares"]) - float(row["sell_shares"]), 4)

    def list_funds(self, execution_date: date | None = None) -> list[dict[str, Any]]:
        effective_date = execution_date or date.today()
        self._refresh_auto_fund_limits()
        fund_rows = self._list_funds()
        execution_context = self._build_same_day_execution_context(
            execution_date=effective_date,
            fund_rows=fund_rows,
        )
        context_by_code = {
            item["fund_code"]: item for item in execution_context["tracked_fund_limits"]
        }
        funds: list[dict[str, Any]] = []
        for row in fund_rows:
            payload = self._serialize_fund_row(row)
            payload.update(context_by_code.get(row["fund_code"], {}))
            funds.append(payload)
        return funds

    def backfill_purchase_fees(self, overwrite: bool = False) -> CommandResult:
        rows = self.conn.execute(
            """
            SELECT DISTINCT t.fund_code, f.fund_name
            FROM transactions t
            JOIN funds f ON f.fund_code = t.fund_code
            WHERE t.status = 'posted'
              AND t.trade_type IN ('buy', 'dca')
            ORDER BY t.fund_code
            """
        ).fetchall()
        if not rows:
            return CommandResult(
                message="没有可回填手续费的买入/定投记录。",
                payload={"fund_count": 0, "transaction_count": 0, "skipped_funds": []},
            )

        updated_transactions = 0
        updated_funds = 0
        skipped_funds: list[str] = []
        for row in rows:
            fund_code = str(row["fund_code"])
            fund_name = str(row["fund_name"])
            rate_pct = self._resolve_purchase_fee_rate_pct(fund_code, fund_name)
            if rate_pct is None or rate_pct <= 0:
                skipped_funds.append(fund_code)
                continue
            updated_funds += 1
            where_fee = "" if overwrite else "AND (fee IS NULL OR ABS(fee) < 1e-9)"
            cursor = self.conn.execute(
                f"""
                UPDATE transactions
                SET fee = ROUND(amount * ? / 100.0, 2)
                WHERE fund_code = ?
                  AND status = 'posted'
                  AND trade_type IN ('buy', 'dca')
                  {where_fee}
                """,
                (float(rate_pct), fund_code),
            )
            updated_transactions += int(cursor.rowcount or 0)

        return CommandResult(
            message=(
                f"手续费回填完成：更新 {updated_transactions} 笔交易，"
                f"命中费率基金 {updated_funds} 只。"
            ),
            payload={
                "fund_count": len(rows),
                "funds_with_fee_rate": updated_funds,
                "transaction_count": updated_transactions,
                "skipped_funds": skipped_funds,
                "overwrite": overwrite,
            },
        )

    def get_price_freshness_diagnostics(self, as_of: date | None = None) -> dict[str, Any]:
        target_date = as_of or date.today()
        fund_rows = self._list_funds()
        items: list[dict[str, Any]] = []
        stale_count = 0
        missing_count = 0
        for fund in fund_rows:
            latest = self._latest_price_row(fund["fund_code"])
            latest_price_date = str(latest["price_date"]) if latest else None
            fetched_at = str(latest["fetched_at"]) if latest and latest["fetched_at"] else None
            if latest_price_date is None:
                lag_days = None
                is_stale = True
                missing_count += 1
            else:
                lag_days = max((target_date - date.fromisoformat(latest_price_date)).days, 0)
                is_stale = lag_days > 0
            if is_stale:
                stale_count += 1
            items.append(
                {
                    "fund_code": str(fund["fund_code"]),
                    "fund_name": str(fund["fund_name"]),
                    "latest_price_date": latest_price_date,
                    "last_fetched_at": fetched_at,
                    "lag_days": lag_days,
                    "is_stale": is_stale,
                }
            )

        items.sort(key=lambda item: (item["lag_days"] is None, -(item["lag_days"] or -1), item["fund_code"]))
        return {
            "as_of_date": target_date.isoformat(),
            "total_funds": len(items),
            "stale_funds": stale_count,
            "missing_price_funds": missing_count,
            "items": items,
        }

    def list_dca_plans(
        self,
        include_inactive: bool = True,
        execution_date: date | None = None,
    ) -> list[dict[str, Any]]:
        effective_date = execution_date or date.today()
        where_clause = "" if include_inactive else "WHERE p.enabled = 1"
        rows = self.conn.execute(
            f"""
            SELECT p.*, f.fund_name, f.default_drop_threshold_pct, f.daily_purchase_limit_amount
            FROM dca_plans p
            JOIN funds f ON f.fund_code = p.fund_code
            {where_clause}
            ORDER BY p.enabled DESC, p.fund_code ASC, p.id ASC
            """
        ).fetchall()
        return [
            self._serialize_dca_plan_row(row, execution_date=effective_date)
            for row in rows
        ]

    def send_test_notification(self) -> CommandResult:
        channels: list[str] = []

        if self.config.notifications.macos_enabled:
            self._send_macos_notification(
                title=f"{self.config.notifications.title_prefix}",
                message="这是一条测试提醒，说明 macOS 本地通知已经可用。",
            )
            channels.append("macos")

        if self.config.email.enabled:
            self._send_email(
                subject=f"[{self.config.notifications.title_prefix}] 测试邮件",
                body="这是一封测试邮件，说明 Fund Tracker 邮件通知已经可用。",
            )
            channels.append("email")

        if not channels:
            raise ValueError("当前没有启用任何通知通道。")

        return CommandResult(
            message=f"已发送测试通知：{', '.join(channels)}",
            payload={"channels": channels},
        )

    def refresh_prices(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for fund in self._list_funds():
            payload = self.price_provider.fetch_payload(fund["fund_code"])
            self._upsert_fund(
                payload.fund_code,
                payload.fund_name,
                float(fund["default_drop_threshold_pct"]),
            )
            latest = payload.latest
            self._upsert_daily_price(
                fund_code=payload.fund_code,
                point=latest,
                source_name=payload.source_name,
            )
            results.append(
                {
                    "fund_code": payload.fund_code,
                    "fund_name": payload.fund_name,
                    "price_date": latest.price_date.isoformat(),
                    "nav": latest.nav,
                    "pct_change_vs_prev": latest.pct_change_vs_prev,
                }
            )
        return results

    def build_portfolio_snapshot(self, as_of: date | None = None) -> dict[str, Any]:
        effective_date = as_of or date.today()
        self._refresh_auto_fund_limits()
        self._ensure_prices_for_tracked_funds(as_of=effective_date)
        positions = self._compute_positions()
        active_dca_plans = self._list_active_dca_plans(execution_date=effective_date)
        same_day_execution_context = self._build_same_day_execution_context(
            execution_date=effective_date,
            active_dca_plans=active_dca_plans,
        )
        execution_context_by_code = {
            item["fund_code"]: item
            for item in same_day_execution_context["tracked_fund_limits"]
        }
        total_market_value = sum(item["market_value"] for item in positions)
        total_cost_basis = sum(item["cost_basis"] for item in positions)
        total_daily_pnl = sum(item["daily_pnl"] for item in positions)
        total_unrealized = sum(item["unrealized_pnl"] for item in positions)
        total_realized = sum(item["realized_pnl"] for item in positions)
        total_net_invested = sum(item["net_invested"] for item in positions)
        total_return_pct = (total_unrealized / total_cost_basis * 100) if total_cost_basis else 0
        priced_positions = [item for item in positions if item["latest_price_date"]]
        latest_price_dates = sorted({str(item["latest_price_date"]) for item in priced_positions})
        one_day_pnl_positions = [item for item in positions if item["daily_pnl_as_of_date"]]
        same_day_priced_positions = [
            item for item in positions if item["latest_price_date"] == effective_date.isoformat()
        ]

        for item in positions:
            item["weight_pct"] = round(
                (item["market_value"] / total_market_value * 100) if total_market_value else 0,
                2,
            )
            fund_context = execution_context_by_code.get(item["fund_code"], {})
            item["today_due_dca_amount"] = fund_context.get("today_due_dca_amount", 0.0)
            item["today_remaining_purchase_capacity"] = fund_context.get(
                "today_remaining_purchase_capacity"
            )
            item["today_limit_exceeded"] = fund_context.get("today_limit_exceeded", False)

        summary = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "portfolio": {
                "position_count": len(positions),
                "total_market_value": round(total_market_value, 2),
                "total_cost_basis": round(total_cost_basis, 2),
                "total_daily_pnl": round(total_daily_pnl, 2),
                "total_unrealized_pnl": round(total_unrealized, 2),
                "total_return_pct": round(total_return_pct, 2),
                "total_realized_pnl": round(total_realized, 2),
                "total_return": round(total_unrealized + total_realized, 2),
                "total_net_invested": round(total_net_invested, 2),
                "valuation_as_of_date_min": latest_price_dates[0] if latest_price_dates else None,
                "valuation_as_of_date_max": latest_price_dates[-1] if latest_price_dates else None,
                "valuation_date_count": len(latest_price_dates),
                "priced_position_count": len(priced_positions),
                "same_day_priced_position_count": len(same_day_priced_positions),
                "one_day_pnl_position_count": len(one_day_pnl_positions),
                "as_of_target_date": effective_date.isoformat(),
                "valuation_mode": "per_fund_latest_nav",
                "daily_pnl_mode": "per_fund_latest_one_day_move",
            },
            "positions": positions,
            "active_dca_plans": active_dca_plans,
            "same_day_execution_context": same_day_execution_context,
        }
        return summary

    def save_snapshot(self, snapshot: dict[str, Any]) -> Path:
        generated_at = datetime.fromisoformat(snapshot["generated_at"])
        dated_path = self.config.snapshot_dir / f"{generated_at.date().isoformat()}-snapshot.json"
        latest_path = self.config.snapshot_dir / "latest-snapshot.json"
        skill_input_path = self.config.snapshot_dir / "latest-snapshot-for-skill.md"

        for target in [dated_path, latest_path]:
            with open(target, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, ensure_ascii=False, indent=2)

        with open(skill_input_path, "w", encoding="utf-8") as handle:
            handle.write(self._format_snapshot_for_skill(snapshot))

        return latest_path

    def _handle_trade(
        self,
        parsed: ParsedCommand,
        raw_text: str,
        command_date: date,
        command_at: datetime,
    ) -> CommandResult:
        identifier = parsed.payload["identifier"]
        fund = self._resolve_fund(identifier)
        payload = self.price_provider.fetch_payload(fund["fund_code"])
        settled = self._resolve_transaction_settlement_dates(
            payload.history,
            order_at=command_at,
            fund_code=fund["fund_code"],
            fund_name=payload.fund_name,
        )
        if settled is None:
            latest_available = payload.history[-1].price_date.isoformat() if payload.history else "未知"
            raise ValueError(
                f"基金 {fund['fund_code']} 在 {command_date.isoformat()} 及之后没有净值数据。"
                f"最新可用净值日为 {latest_available}，请在指令里写日期，例如："
                f"买入 {fund['fund_code']} 10 {latest_available}"
            )
        confirm_point, effective_from_date, used_fallback_price = settled

        self._upsert_fund(
            payload.fund_code,
            payload.fund_name,
            float(fund["default_drop_threshold_pct"]),
        )
        self._upsert_daily_price(payload.fund_code, confirm_point, payload.source_name)

        if parsed.payload["value_type"] == "shares":
            shares = parsed.payload["value"]
            amount = round(shares * confirm_point.nav, 2)
        else:
            amount = parsed.payload["value"]
            shares = round(amount / confirm_point.nav, 4)

        if parsed.payload["trade_type"] == "sell":
            current_shares = self._current_shares(fund["fund_code"])
            if shares > current_shares + 1e-6:
                raise ValueError(
                    f"卖出份额 {shares} 超过当前可用份额 {round(current_shares, 4)}：{fund['fund_code']}"
                )
        fee = (
            self._compute_purchase_fee_amount(fund["fund_code"], payload.fund_name, amount)
            if parsed.payload["trade_type"] == "buy"
            else 0.0
        )

        self._insert_transaction(
            fund_code=fund["fund_code"],
            trade_date=confirm_point.price_date.isoformat(),
            trade_type=parsed.payload["trade_type"],
            amount=amount,
            nav=confirm_point.nav,
            shares=shares,
            fee=fee,
            source="manual",
            status="posted",
            note=None,
            raw_text=raw_text,
            plan_id=None,
            order_date=command_date.isoformat(),
            order_at=command_at.isoformat(timespec="seconds"),
            confirm_nav_date=confirm_point.price_date.isoformat(),
            effective_from_date=effective_from_date,
        )

        action_text = "买入" if parsed.payload["trade_type"] == "buy" else "卖出"
        settlement_hint = (
            f"（当日后暂无新净值，自动按最近净值日 {confirm_point.price_date.isoformat()} 确认）"
            if used_fallback_price
            else ""
        )
        return CommandResult(
            message=(
                f"已记录{action_text}：{fund['fund_code']} {fund['fund_name']} "
                f"{amount:.2f} 元，确认净值日 {confirm_point.price_date.isoformat()}，"
                f"净值 {confirm_point.nav:.4f}，份额 {shares:.4f}，手续费 {fee:.2f} 元"
                f"{settlement_hint}"
            ),
            payload={
                "fund_code": fund["fund_code"],
                "fund_name": fund["fund_name"],
                "amount": round(amount, 2),
                "nav": confirm_point.nav,
                "shares": round(shares, 4),
                "trade_type": parsed.payload["trade_type"],
                "trade_date": confirm_point.price_date.isoformat(),
                "order_date": command_date.isoformat(),
                "order_at": command_at.isoformat(timespec="seconds"),
                "confirm_nav_date": confirm_point.price_date.isoformat(),
                "effective_from_date": effective_from_date,
                "fee": round(fee, 2),
                "used_fallback_price": used_fallback_price,
            },
        )

    def _handle_create_dca(self, parsed: ParsedCommand, command_date: date) -> CommandResult:
        fund = self._resolve_fund(parsed.payload["identifier"])
        now = _now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO dca_plans (
                    fund_code, amount, frequency, run_rule, enabled, start_date,
                    end_date, note, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?, NULL, NULL, ?, ?)
                """,
                (
                    fund["fund_code"],
                    parsed.payload["amount"],
                    parsed.payload["frequency"],
                    parsed.payload["run_rule"],
                    command_date.isoformat(),
                    now,
                    now,
                ),
            )

        return CommandResult(
            message=(
                f"已新增定投：{fund['fund_code']} {fund['fund_name']} "
                f"{parsed.payload['amount']:.2f} 元 / {parsed.payload['run_rule']}"
            ),
            payload={
                "fund_code": fund["fund_code"],
                "amount": parsed.payload["amount"],
                "frequency": parsed.payload["frequency"],
                "run_rule": parsed.payload["run_rule"],
            },
        )

    def _set_dca_enabled(self, identifier: str, enabled: bool) -> CommandResult:
        fund = self._resolve_fund(identifier)
        now = _now_iso()
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE dca_plans
                SET enabled = ?, updated_at = ?
                WHERE fund_code = ? AND enabled != ?
                """,
                (1 if enabled else 0, now, fund["fund_code"], 1 if enabled else 0),
            )

        action_text = "恢复" if enabled else "暂停"
        if cursor.rowcount == 0:
            return CommandResult(
                message=f"没有需要{action_text}的定投计划：{fund['fund_code']} {fund['fund_name']}",
                payload={"fund_code": fund["fund_code"], "changed": False},
            )
        return CommandResult(
            message=f"已{action_text}定投：{fund['fund_code']} {fund['fund_name']}",
            payload={"fund_code": fund["fund_code"], "changed": True},
        )

    def _cancel_dca(self, identifier: str) -> CommandResult:
        fund = self._resolve_fund(identifier)
        now = _now_iso()
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE dca_plans
                SET enabled = 0, end_date = COALESCE(end_date, ?), updated_at = ?
                WHERE fund_code = ? AND enabled = 1
                """,
                (date.today().isoformat(), now, fund["fund_code"]),
            )

        return CommandResult(
            message=(
                f"已取消定投：{fund['fund_code']} {fund['fund_name']}"
                if cursor.rowcount
                else f"没有可取消的定投计划：{fund['fund_code']} {fund['fund_name']}"
            ),
            payload={"fund_code": fund["fund_code"], "changed": bool(cursor.rowcount)},
        )

    def _materialize_dca_transactions(self, target_date: date) -> list[str]:
        created_dates: list[str] = []
        for plan in self._list_active_dca_plans():
            payload = self.price_provider.fetch_payload(plan["fund_code"])
            market_point = _latest_point_on_or_before(payload.history, target_date)
            if market_point is None:
                continue
            if not self._plan_matches_market_date(plan, market_point.price_date):
                continue
            if self._dca_transaction_exists(plan["id"], market_point.price_date.isoformat()):
                continue

            self._upsert_fund(
                payload.fund_code,
                payload.fund_name,
                float(plan["default_drop_threshold_pct"]),
            )
            self._upsert_daily_price(payload.fund_code, market_point, payload.source_name)
            shares = round(float(plan["amount"]) / market_point.nav, 4)
            fee = self._compute_purchase_fee_amount(plan["fund_code"], payload.fund_name, float(plan["amount"]))
            self._insert_transaction(
                fund_code=plan["fund_code"],
                trade_date=market_point.price_date.isoformat(),
                trade_type="dca",
                amount=float(plan["amount"]),
                nav=market_point.nav,
                shares=shares,
                fee=fee,
                source="auto_dca",
                status="posted",
                note=f"自动定投 {plan['run_rule']}",
                raw_text=f"auto dca {plan['run_rule']}",
                plan_id=int(plan["id"]),
                order_date=target_date.isoformat(),
                order_at=datetime.combine(target_date, time(9, 30)).isoformat(timespec="seconds"),
                confirm_nav_date=market_point.price_date.isoformat(),
                effective_from_date=_next_price_date_after(payload.history, market_point.price_date),
            )
            created_dates.append(market_point.price_date.isoformat())
        return created_dates

    def _check_alerts(self) -> list[dict[str, Any]]:
        alerts_sent: list[dict[str, Any]] = []
        snapshot = self.build_portfolio_snapshot()
        positions_by_code = {item["fund_code"]: item for item in snapshot["positions"]}

        for fund in self._list_funds():
            latest_price = self._latest_price_row(fund["fund_code"])
            if latest_price is None:
                continue
            drop_threshold = float(fund["default_drop_threshold_pct"])
            pct_change = latest_price["pct_change_vs_prev"]
            if pct_change is None or float(pct_change) > -drop_threshold:
                continue
            if self._alert_exists(fund["fund_code"], latest_price["price_date"], "drop_threshold"):
                continue

            position = positions_by_code.get(fund["fund_code"])
            message = self._build_alert_message(fund, latest_price, position)
            channel_statuses: list[str] = []
            if self.config.email.enabled:
                try:
                    self._send_email(
                        subject=f"[基金补仓提醒] {fund['fund_name']} ({fund['fund_code']})",
                        body=message,
                    )
                    channel_statuses.append("email:sent")
                except Exception as exc:
                    channel_statuses.append(f"email:failed:{exc}")

            if self.config.notifications.macos_enabled:
                try:
                    self._send_macos_notification(
                        title=f"{self.config.notifications.title_prefix} - 达到补仓阈值",
                        message=(
                            f"{fund['fund_name']}({fund['fund_code']}) "
                            f"下跌 {float(pct_change):.2f}%"
                        ),
                    )
                    channel_statuses.append("macos:sent")
                except Exception as exc:
                    channel_statuses.append(f"macos:failed:{exc}")

            delivery_status = ",".join(channel_statuses) if channel_statuses else "skipped"

            self._insert_alert(
                fund_code=fund["fund_code"],
                alert_date=latest_price["price_date"],
                alert_type="drop_threshold",
                trigger_value=float(pct_change),
                delivery_status=delivery_status,
                message=message,
            )
            alerts_sent.append(
                {
                    "fund_code": fund["fund_code"],
                    "fund_name": fund["fund_name"],
                    "alert_date": latest_price["price_date"],
                    "pct_change_vs_prev": float(pct_change),
                    "delivery_status": delivery_status,
                }
            )

        return alerts_sent

    def _resolve_transaction_settlement_dates(
        self,
        history: list[PricePoint],
        order_at: datetime,
        fund_code: str,
        fund_name: str,
    ) -> tuple[PricePoint, str, bool] | None:
        if not history:
            return None
        rule = self._resolve_settlement_rule(fund_code=fund_code, fund_name=fund_name)
        confirm_point = _resolve_confirm_point_with_rule(
            history=history,
            order_at=order_at,
            cutoff_time=rule["cutoff_time"],
            confirm_trade_day_lag=int(rule["confirm_trade_day_lag"]),
        )
        used_fallback_price = False
        if confirm_point is None:
            confirm_point = _latest_point_on_or_before(history, order_at.date())
            if confirm_point is None:
                return None
            used_fallback_price = True
        effective_from_date = _resolve_effective_from_date_with_rule(
            history=history,
            confirm_date=confirm_point.price_date,
            effective_trade_day_lag_after_confirm=int(rule["effective_trade_day_lag_after_confirm"]),
        )
        return confirm_point, effective_from_date, used_fallback_price

    def _resolve_settlement_rule(self, fund_code: str, fund_name: str) -> dict[str, Any]:
        default_rule = {
            "cutoff_time": "15:00",
            "confirm_trade_day_lag": 0,
            "effective_trade_day_lag_after_confirm": 1,
        }
        try:
            raw = self._external_research_engine().lookup_fund_settlement_rule(
                fund_code=fund_code,
                fund_name=fund_name,
            )
        except Exception:
            return default_rule
        cutoff = str(raw.get("cutoff_time", "15:00")).strip() or "15:00"
        confirm_lag = int(raw.get("confirm_trade_day_lag", 0) or 0)
        effective_lag = int(raw.get("effective_trade_day_lag_after_confirm", 1) or 1)
        return {
            "cutoff_time": cutoff,
            "confirm_trade_day_lag": max(confirm_lag, 0),
            "effective_trade_day_lag_after_confirm": max(effective_lag, 1),
        }

    def _order_datetime_for_command(self, command_date: date) -> datetime:
        today = date.today()
        if command_date == today:
            return datetime.now()
        return datetime.combine(command_date, time(0, 0))

    def _ensure_prices_for_tracked_funds(self, as_of: date | None = None) -> None:
        target_date = as_of or date.today()
        stale_codes: list[str] = []
        rows = self.conn.execute(
            """
            SELECT DISTINCT fund_code
            FROM transactions
            WHERE status = 'posted'
            ORDER BY fund_code
            """
        ).fetchall()
        for row in rows:
            latest = self._latest_price_row(row["fund_code"])
            latest_date = date.fromisoformat(latest["price_date"]) if latest else None
            if latest_date is None or latest_date < target_date:
                stale_codes.append(row["fund_code"])

        for fund_code in stale_codes:
            fund = self.conn.execute(
                "SELECT * FROM funds WHERE fund_code = ?",
                (fund_code,),
            ).fetchone()
            if fund is None:
                continue
            payload = self.price_provider.fetch_payload(fund_code)
            self._upsert_fund(
                payload.fund_code,
                payload.fund_name,
                float(fund["default_drop_threshold_pct"]),
            )
            self._upsert_daily_price(payload.fund_code, payload.latest, payload.source_name)

    def _compute_positions(self) -> list[dict[str, Any]]:
        positions: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "fund_code": "",
                "fund_name": "",
                "daily_purchase_limit_amount": None,
                "shares": 0.0,
                "cost_basis": 0.0,
                "realized_pnl": 0.0,
                "gross_buy_amount": 0.0,
                "gross_sell_amount": 0.0,
            }
        )

        transactions = self.conn.execute(
            """
            SELECT t.*, f.fund_name, f.daily_purchase_limit_amount
            FROM transactions t
            JOIN funds f ON f.fund_code = t.fund_code
            WHERE t.status = 'posted'
            ORDER BY t.trade_date ASC, t.id ASC
            """
        ).fetchall()

        for row in transactions:
            item = positions[row["fund_code"]]
            item["fund_code"] = row["fund_code"]
            item["fund_name"] = row["fund_name"]
            item["daily_purchase_limit_amount"] = (
                round(float(row["daily_purchase_limit_amount"]), 2)
                if row["daily_purchase_limit_amount"] is not None
                else None
            )
            amount = float(row["amount"])
            shares = float(row["shares"])

            if row["trade_type"] in {"buy", "dca", "initial"}:
                item["shares"] += shares
                item["cost_basis"] += amount + float(row["fee"])
                item["gross_buy_amount"] += amount
            elif row["trade_type"] == "sell":
                if item["shares"] <= 0:
                    raise ValueError(f"基金 {row['fund_code']} 存在无持仓卖出流水")
                average_cost = item["cost_basis"] / item["shares"]
                cost_removed = average_cost * shares
                item["shares"] -= shares
                item["cost_basis"] -= cost_removed
                item["gross_sell_amount"] += amount
                item["realized_pnl"] += amount - float(row["fee"]) - cost_removed

        position_list: list[dict[str, Any]] = []
        for item in positions.values():
            latest_price = self._latest_price_row(item["fund_code"])
            latest_nav = float(latest_price["nav"]) if latest_price else 0.0
            latest_price_date = latest_price["price_date"] if latest_price else None
            market_value = item["shares"] * latest_nav
            unrealized_pnl = market_value - item["cost_basis"]
            average_cost = item["cost_basis"] / item["shares"] if item["shares"] else 0.0
            daily_pct_change = (
                float(latest_price["pct_change_vs_prev"])
                if latest_price and latest_price["pct_change_vs_prev"] is not None
                else None
            )
            daily_shares = self._shares_held_before_price_date(
                item["fund_code"],
                latest_price_date,
                item["shares"],
            )
            daily_pnl = self._compute_daily_pnl(latest_nav, daily_pct_change, daily_shares)
            position_list.append(
                {
                    "fund_code": item["fund_code"],
                    "fund_name": item["fund_name"],
                    "daily_purchase_limit_amount": item["daily_purchase_limit_amount"],
                    "shares": round(item["shares"], 4),
                    "average_cost_nav": round(average_cost, 4),
                    "cost_basis": round(item["cost_basis"], 2),
                    "market_value": round(market_value, 2),
                    "latest_nav": round(latest_nav, 4),
                    "latest_price_date": latest_price_date,
                    "valuation_as_of_date": latest_price_date,
                    "daily_pct_change": daily_pct_change,
                    "daily_pnl_as_of_date": latest_price_date if daily_pct_change is not None else None,
                    "daily_pnl": round(daily_pnl, 2),
                    "unrealized_pnl": round(unrealized_pnl, 2),
                    "realized_pnl": round(item["realized_pnl"], 2),
                    "net_invested": round(item["gross_buy_amount"] - item["gross_sell_amount"], 2),
                    "return_pct": round(
                        (unrealized_pnl / item["cost_basis"] * 100) if item["cost_basis"] else 0,
                        2,
                    ),
                }
            )

        position_list.sort(key=lambda record: record["market_value"], reverse=True)
        return position_list

    def _build_local_analysis(self, snapshot: dict[str, Any]) -> str:
        positions = snapshot["positions"]
        if not positions:
            return "当前没有持仓，无法生成分析。"

        lines = [
            "# 本地持仓分析",
            "",
            f"- 组合总市值：{snapshot['portfolio']['total_market_value']:.2f}",
            f"- 浮动盈亏：{snapshot['portfolio']['total_unrealized_pnl']:.2f}",
            f"- 已实现盈亏：{snapshot['portfolio']['total_realized_pnl']:.2f}",
            "",
            "## 风险提示",
        ]

        top_position = positions[0]
        if top_position["weight_pct"] >= 35:
            lines.append(
                f"- 第一大持仓 `{top_position['fund_name']}` 占比 {top_position['weight_pct']:.2f}%，集中度偏高。"
            )
        else:
            lines.append("- 当前组合未出现单只基金明显过度集中的情况。")

        drawdown_candidates = [
            item for item in positions
            if item["daily_pct_change"] is not None
            and item["daily_pct_change"] <= -abs(self._threshold_for(item["fund_code"]))
        ]
        if drawdown_candidates:
            codes = "、".join(item["fund_code"] for item in drawdown_candidates)
            lines.append(f"- 今日达到补仓阈值的基金：{codes}")
        else:
            lines.append("- 今日没有基金触发补仓阈值。")

        lines.extend(["", "## 优先观察", ""])
        for item in positions[:3]:
            lines.append(
                f"- `{item['fund_code']}` {item['fund_name']}：仓位 {item['weight_pct']:.2f}%，"
                f"日涨跌 {item['daily_pct_change'] if item['daily_pct_change'] is not None else 'N/A'}%，"
                f"收益率 {item['return_pct']:.2f}%。"
            )
        return "\n".join(lines)

    def _format_snapshot_for_skill(self, snapshot: dict[str, Any]) -> str:
        lines = [
            "# Portfolio Snapshot for Skill",
            "",
            f"- Generated: {snapshot['generated_at']}",
            f"- Total Market Value: {snapshot['portfolio']['total_market_value']}",
            f"- Total Unrealized PnL: {snapshot['portfolio']['total_unrealized_pnl']}",
            f"- Total Realized PnL: {snapshot['portfolio']['total_realized_pnl']}",
            "",
            "## Positions",
        ]
        for item in snapshot["positions"]:
            lines.append(
                "- "
                f"{item['fund_code']} | {item['fund_name']} | shares={item['shares']} | "
                f"weight={item['weight_pct']}% | latest_nav={item['latest_nav']} | "
                f"daily_pct_change={item['daily_pct_change']} | return_pct={item['return_pct']}% | "
                f"daily_limit={item.get('daily_purchase_limit_amount')}"
            )
        lines.extend(["", "## Active DCA Plans"])
        for plan in snapshot["active_dca_plans"]:
            lines.append(
                f"- {plan['fund_code']} | amount={plan['amount']} | frequency={plan['frequency']} | "
                f"rule={plan['run_rule']} | due_today={plan.get('is_due_today')} | "
                f"daily_limit={plan.get('daily_purchase_limit_amount')}"
            )
        return "\n".join(lines)

    def _resolve_fund(self, identifier: str) -> sqlite3.Row:
        identifier = identifier.strip()
        if identifier.isdigit():
            row = self.conn.execute(
                "SELECT * FROM funds WHERE fund_code = ?",
                (identifier,),
            ).fetchone()
            if row:
                return row

            payload = self.price_provider.fetch_payload(identifier)
            self._upsert_fund(
                payload.fund_code,
                payload.fund_name,
                self.config.default_drop_threshold_pct,
            )
            row = self.conn.execute(
                "SELECT * FROM funds WHERE fund_code = ?",
                (identifier,),
            ).fetchone()
            if row:
                return row

        matches = self.conn.execute(
            "SELECT * FROM funds WHERE fund_name LIKE ? ORDER BY fund_code",
            (f"%{identifier}%",),
        ).fetchall()
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            options = ", ".join(f"{row['fund_code']} {row['fund_name']}" for row in matches[:5])
            raise ValueError(f"基金名称匹配到多个结果，请改用代码：{options}")
        raise ValueError(f"无法识别基金：{identifier}。请先导入初始持仓或直接使用基金代码。")

    def _upsert_fund(
        self,
        fund_code: str,
        fund_name: str,
        drop_threshold_pct: float,
        purchase_fee_rate_pct: float | None = None,
    ) -> None:
        now = _now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO funds (
                    fund_code, fund_name, fund_type, enabled,
                    default_drop_threshold_pct, purchase_fee_rate_pct, created_at, updated_at
                ) VALUES (?, ?, 'fund', 1, ?, ?, ?, ?)
                ON CONFLICT(fund_code) DO UPDATE SET
                    fund_name = excluded.fund_name,
                    default_drop_threshold_pct = COALESCE(funds.default_drop_threshold_pct, excluded.default_drop_threshold_pct),
                    purchase_fee_rate_pct = COALESCE(excluded.purchase_fee_rate_pct, funds.purchase_fee_rate_pct),
                    updated_at = excluded.updated_at
                """,
                (fund_code, fund_name, drop_threshold_pct, purchase_fee_rate_pct, now, now),
            )

    def _compute_purchase_fee_amount(self, fund_code: str, fund_name: str, amount: float) -> float:
        if amount <= 0:
            return 0.0
        rate_pct = self._resolve_purchase_fee_rate_pct(fund_code, fund_name)
        if rate_pct is None or rate_pct <= 0:
            return 0.0
        return round(amount * rate_pct / 100, 2)

    def _resolve_purchase_fee_rate_pct(self, fund_code: str, fund_name: str) -> float | None:
        row = self.conn.execute(
            "SELECT purchase_fee_rate_pct FROM funds WHERE fund_code = ?",
            (fund_code,),
        ).fetchone()
        if row is not None and row["purchase_fee_rate_pct"] is not None:
            return round(float(row["purchase_fee_rate_pct"]), 4)

        try:
            rate_pct = self._external_research_engine().lookup_fund_purchase_fee_rate(
                fund_code=fund_code,
                fund_name=fund_name,
            )
        except Exception:
            return None
        if rate_pct is None:
            return None
        normalized_rate = round(float(rate_pct), 4)
        with self.conn:
            self.conn.execute(
                """
                UPDATE funds
                SET purchase_fee_rate_pct = ?, updated_at = ?
                WHERE fund_code = ?
                """,
                (normalized_rate, _now_iso(), fund_code),
            )
        return normalized_rate

    def _insert_transaction(
        self,
        fund_code: str,
        trade_date: str,
        trade_type: str,
        amount: float,
        nav: float,
        shares: float,
        fee: float,
        source: str,
        status: str,
        note: str | None,
        raw_text: str | None,
        plan_id: int | None,
        order_date: str | None = None,
        order_at: str | None = None,
        confirm_nav_date: str | None = None,
        effective_from_date: str | None = None,
    ) -> None:
        effective_order_date = order_date or trade_date
        effective_order_at = order_at or datetime.combine(
            date.fromisoformat(effective_order_date),
            time(0, 0),
        ).isoformat(timespec="seconds")
        effective_confirm_nav_date = confirm_nav_date or trade_date
        effective_from = effective_from_date or trade_date
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO transactions (
                    fund_code, trade_date, trade_type, amount, nav, shares, fee,
                    source, status, note, raw_text, plan_id, order_date,
                    order_at, confirm_nav_date, effective_from_date, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fund_code,
                    trade_date,
                    trade_type,
                    round(amount, 2),
                    round(nav, 4),
                    round(shares, 4),
                    round(fee, 2),
                    source,
                    status,
                    note,
                    raw_text,
                    plan_id,
                    effective_order_date,
                    effective_order_at,
                    effective_confirm_nav_date,
                    effective_from,
                    _now_iso(),
                ),
            )

    def _upsert_daily_price(self, fund_code: str, point: PricePoint, source_name: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO daily_prices (
                    fund_code, price_date, nav, pct_change_vs_prev, source_name, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(fund_code, price_date) DO UPDATE SET
                    nav = excluded.nav,
                    pct_change_vs_prev = excluded.pct_change_vs_prev,
                    source_name = excluded.source_name,
                    fetched_at = excluded.fetched_at
                """,
                (
                    fund_code,
                    point.price_date.isoformat(),
                    round(point.nav, 4),
                    point.pct_change_vs_prev,
                    source_name,
                    _now_iso(),
                ),
            )

    def _insert_alert(
        self,
        fund_code: str,
        alert_date: str,
        alert_type: str,
        trigger_value: float,
        delivery_status: str,
        message: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO alerts (
                    fund_code, alert_date, alert_type, trigger_value,
                    delivery_status, message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fund_code,
                    alert_date,
                    alert_type,
                    trigger_value,
                    delivery_status,
                    message,
                    _now_iso(),
                ),
            )

    def _save_analysis_report(
        self,
        report_type: str,
        input_snapshot: dict[str, Any],
        skill_name: str,
        report_body: str,
        report_date: date | None = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO analysis_reports (
                    report_date, report_type, input_snapshot, skill_name, report_body, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (report_date or date.today()).isoformat(),
                    report_type,
                    json.dumps(input_snapshot, ensure_ascii=False),
                    skill_name,
                    report_body,
                    _now_iso(),
                ),
            )

    def _send_daily_opportunity_notifications(
        self,
        report_date: date,
        summary: str,
        opportunities: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        cooldown_results: list[dict[str, Any]] = []
        fresh_opportunities: list[dict[str, Any]] = []
        for item in opportunities:
            fund_code = str(item.get("fund_code") or "").strip()
            fund_name = str(item.get("fund_name") or fund_code).strip() or fund_code
            if not fund_code:
                continue
            if self._alert_exists(fund_code, report_date.isoformat(), "daily_opportunity"):
                continue
            if self._recent_alert_exists(
                fund_code=fund_code,
                alert_date=report_date,
                alert_type="daily_opportunity",
                lookback_days=3,
            ):
                cooldown_results.append(
                    {
                        "fund_code": fund_code,
                        "fund_name": fund_name,
                        "delivery_status": "cooldown:3d",
                        "alert_date": report_date.isoformat(),
                    }
                )
                continue
            fresh_opportunities.append(item)
        if not fresh_opportunities:
            return cooldown_results

        headline = "；".join(
            self._format_daily_opportunity_brief(item) for item in fresh_opportunities[:2]
        )
        message = headline if not summary else f"{headline}。{summary}"

        channel_statuses: list[str] = []
        if self.config.email.enabled:
            try:
                self._send_email(
                    subject=f"[{self.config.notifications.title_prefix}] 今日强机会",
                    body=message,
                )
                channel_statuses.append("email:sent")
            except Exception as exc:
                channel_statuses.append(f"email:failed:{exc}")

        if self.config.notifications.macos_enabled:
            try:
                self._send_macos_notification(
                    title=f"{self.config.notifications.title_prefix} - 今日强机会",
                    message=message,
                )
                channel_statuses.append("macos:sent")
            except Exception as exc:
                channel_statuses.append(f"macos:failed:{exc}")

        delivery_status = ",".join(channel_statuses) if channel_statuses else "skipped"
        results: list[dict[str, Any]] = []
        for item in fresh_opportunities:
            fund_code = str(item.get("fund_code") or "").strip()
            fund_name = str(item.get("fund_name") or fund_code).strip() or fund_code
            self._insert_alert(
                fund_code=fund_code,
                alert_date=report_date.isoformat(),
                alert_type="daily_opportunity",
                trigger_value=1.0,
                delivery_status=delivery_status,
                message=message,
            )
            results.append(
                {
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "delivery_status": delivery_status,
                    "alert_date": report_date.isoformat(),
                }
            )
        return cooldown_results + results

    def _format_daily_opportunity_brief(self, opportunity: dict[str, Any]) -> str:
        fund_name = str(opportunity.get("fund_name") or opportunity.get("fund_code") or "").strip()
        fund_code = str(opportunity.get("fund_code") or "").strip()
        amount = opportunity.get("suggested_amount")
        amount_text = ""
        if amount not in (None, ""):
            try:
                amount_text = f" {round(float(amount), 2):.0f}元"
            except (TypeError, ValueError):
                amount_text = ""
        return f"{fund_name}({fund_code}){amount_text}"

    def list_analysis_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT id, report_date, report_type, input_snapshot, skill_name, report_body, created_at
            FROM analysis_reports
            WHERE report_type NOT IN ('daily', 'external_daily', 'external_weekly', 'external_cash_plan')
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_priority_industry_watchlist(self) -> dict[str, Any]:
        return self._external_research_engine().describe_priority_industry_watchlist()

    def update_priority_industry_watchlist(self, themes: list[str]) -> dict[str, Any]:
        return self._external_research_engine().update_priority_industry_watchlist(themes)

    def _external_research_engine(self) -> ExternalResearchEngine:
        source_config_path = self.config.config_path.parent / "research_sources.yaml"
        cache_dir = self.config.snapshot_dir.parent / "research_cache"
        return ExternalResearchEngine(source_config_path, cache_dir)

    def _codex_monthly_briefing_runner(self) -> CodexMonthlyBriefingRunner:
        project_root = self.config.config_path.parent.parent
        runtime_dir = self.config.snapshot_dir.parent / "codex_briefings"
        return CodexMonthlyBriefingRunner(project_root=project_root, runtime_dir=runtime_dir)

    def _latest_analysis_report(self, report_type: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT id, report_type, skill_name, report_body, created_at
            FROM analysis_reports
            WHERE report_type = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (report_type,),
        ).fetchone()
        return dict(row) if row else None

    def _is_month_end(self, value: date) -> bool:
        return (value + timedelta(days=1)).month != value.month

    def _list_funds(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT *
            FROM funds f
            WHERE f.enabled = 1
              AND (
                EXISTS (
                    SELECT 1
                    FROM transactions t
                    WHERE t.fund_code = f.fund_code
                      AND t.status = 'posted'
                )
                OR EXISTS (
                    SELECT 1
                    FROM dca_plans p
                    WHERE p.fund_code = f.fund_code
                )
              )
            ORDER BY f.fund_code
            """
        ).fetchall()

    def _refresh_auto_fund_limits(self) -> None:
        fund_rows = self.conn.execute(
            """
            SELECT *
            FROM funds f
            WHERE f.enabled = 1
              AND (
                EXISTS (
                    SELECT 1
                    FROM transactions t
                    WHERE t.fund_code = f.fund_code
                      AND t.status = 'posted'
                )
                OR EXISTS (
                    SELECT 1
                    FROM dca_plans p
                    WHERE p.fund_code = f.fund_code
                )
              )
            ORDER BY f.fund_code
            """
        ).fetchall()
        if not fund_rows:
            return

        engine = self._external_research_engine()
        updates: list[tuple[float | None, str, str]] = []
        for row in fund_rows:
            constraint = engine.lookup_fund_trade_constraint(
                row["fund_code"],
                row["fund_name"],
            )
            if not constraint.get("fetch_succeeded"):
                continue

            next_limit = constraint.get("daily_purchase_limit_amount")
            normalized_next_limit = (
                round(float(next_limit), 2) if next_limit is not None else None
            )
            current_limit = (
                round(float(row["daily_purchase_limit_amount"]), 2)
                if row["daily_purchase_limit_amount"] is not None
                else None
            )
            if current_limit == normalized_next_limit:
                continue
            updates.append((normalized_next_limit, _now_iso(), row["fund_code"]))

        if not updates:
            return
        with self.conn:
            self.conn.executemany(
                """
                UPDATE funds
                SET daily_purchase_limit_amount = ?, updated_at = ?
                WHERE fund_code = ?
                """,
                updates,
            )

    def _list_active_dca_plans(self, execution_date: date | None = None) -> list[dict[str, Any]]:
        return self.list_dca_plans(include_inactive=False, execution_date=execution_date)

    def _serialize_fund_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["enabled"] = bool(payload.get("enabled", 0))
        limit_amount = payload.get("daily_purchase_limit_amount")
        payload["daily_purchase_limit_amount"] = (
            round(float(limit_amount), 2) if limit_amount is not None else None
        )
        fee_rate = payload.get("purchase_fee_rate_pct")
        payload["purchase_fee_rate_pct"] = (
            round(float(fee_rate), 4) if fee_rate is not None else None
        )
        payload["default_drop_threshold_pct"] = round(
            float(payload["default_drop_threshold_pct"]),
            2,
        )
        return payload

    def _serialize_dca_plan_row(
        self,
        row: sqlite3.Row,
        execution_date: date,
    ) -> dict[str, Any]:
        payload = dict(row)
        payload["enabled"] = bool(payload.get("enabled", 0))
        payload["amount"] = round(float(payload["amount"]), 2)
        limit_amount = payload.get("daily_purchase_limit_amount")
        payload["daily_purchase_limit_amount"] = (
            round(float(limit_amount), 2) if limit_amount is not None else None
        )
        is_due_today = payload["enabled"] and self._plan_matches_market_date(payload, execution_date)
        payload["is_due_today"] = is_due_today
        payload["today_reserved_amount"] = payload["amount"] if is_due_today else 0.0
        payload["today_remaining_purchase_capacity"] = (
            None
            if payload["daily_purchase_limit_amount"] is None
            else round(
                max(payload["daily_purchase_limit_amount"] - payload["today_reserved_amount"], 0.0),
                2,
            )
        )
        payload["today_limit_exceeded"] = bool(
            payload["daily_purchase_limit_amount"] is not None
            and payload["today_reserved_amount"] > payload["daily_purchase_limit_amount"]
        )
        return payload

    def _build_same_day_execution_context(
        self,
        execution_date: date,
        active_dca_plans: list[dict[str, Any]] | None = None,
        fund_rows: list[sqlite3.Row] | None = None,
    ) -> dict[str, Any]:
        plans = (
            active_dca_plans
            if active_dca_plans is not None
            else self._list_active_dca_plans(execution_date=execution_date)
        )
        today_due_dca_plans = [plan for plan in plans if plan.get("is_due_today")]
        due_amount_by_code: dict[str, float] = defaultdict(float)
        for plan in today_due_dca_plans:
            due_amount_by_code[str(plan["fund_code"])] += float(plan.get("today_reserved_amount", 0.0))

        rows = fund_rows if fund_rows is not None else self._list_funds()
        tracked_fund_limits: list[dict[str, Any]] = []
        for row in rows:
            limit_amount = (
                round(float(row["daily_purchase_limit_amount"]), 2)
                if row["daily_purchase_limit_amount"] is not None
                else None
            )
            today_due_dca_amount = round(due_amount_by_code.get(row["fund_code"], 0.0), 2)
            tracked_fund_limits.append(
                {
                    "fund_code": row["fund_code"],
                    "fund_name": row["fund_name"],
                    "daily_purchase_limit_amount": limit_amount,
                    "today_due_dca_amount": today_due_dca_amount,
                    "today_remaining_purchase_capacity": (
                        None
                        if limit_amount is None
                        else round(max(limit_amount - today_due_dca_amount, 0.0), 2)
                    ),
                    "today_limit_exceeded": bool(
                        limit_amount is not None and today_due_dca_amount > limit_amount
                    ),
                }
            )

        return {
            "execution_date": execution_date.isoformat(),
            "today_due_dca_total_amount": round(
                sum(float(plan.get("today_reserved_amount", 0.0)) for plan in today_due_dca_plans),
                2,
            ),
            "today_due_dca_plans": today_due_dca_plans,
            "tracked_fund_limits": tracked_fund_limits,
        }

    def _latest_price_row(self, fund_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT *
            FROM daily_prices
            WHERE fund_code = ?
            ORDER BY price_date DESC
            LIMIT 1
            """,
            (fund_code,),
        ).fetchone()

    def _shares_held_before_price_date(
        self,
        fund_code: str,
        price_date: str | None,
        current_shares: float,
    ) -> float:
        if not price_date:
            return current_shares

        row = self.conn.execute(
            """
            SELECT
                COALESCE(
                    SUM(
                        CASE
                            WHEN trade_type IN ('buy', 'dca', 'initial')
                             AND COALESCE(effective_from_date, trade_date) > ?
                            THEN shares
                            ELSE 0
                        END
                    ),
                    0
                ) AS buy_shares,
                COALESCE(
                    SUM(
                        CASE
                            WHEN trade_type = 'sell'
                             AND COALESCE(effective_from_date, trade_date) > ?
                            THEN shares
                            ELSE 0
                        END
                    ),
                    0
                ) AS sell_shares
            FROM transactions
            WHERE fund_code = ? AND status = 'posted'
            """,
            (price_date, price_date, fund_code),
        ).fetchone()
        buy_shares = float(row["buy_shares"]) if row else 0.0
        sell_shares = float(row["sell_shares"]) if row else 0.0
        return max(current_shares - buy_shares + sell_shares, 0.0)

    def _compute_daily_pnl(
        self,
        latest_nav: float,
        daily_pct_change: float | None,
        effective_shares: float,
    ) -> float:
        if daily_pct_change is None or latest_nav <= 0 or effective_shares <= 0:
            return 0.0
        previous_nav = latest_nav / (1 + daily_pct_change / 100)
        return (latest_nav - previous_nav) * effective_shares

    def _alert_exists(self, fund_code: str, alert_date: str, alert_type: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM alerts
            WHERE fund_code = ? AND alert_date = ? AND alert_type = ?
            LIMIT 1
            """,
            (fund_code, alert_date, alert_type),
        ).fetchone()
        return row is not None

    def _recent_alert_exists(
        self,
        fund_code: str,
        alert_date: date,
        alert_type: str,
        lookback_days: int,
    ) -> bool:
        start_date = (alert_date - timedelta(days=max(lookback_days, 0))).isoformat()
        end_date = (alert_date - timedelta(days=1)).isoformat()
        if end_date < start_date:
            return False
        row = self.conn.execute(
            """
            SELECT 1
            FROM alerts
            WHERE fund_code = ? AND alert_type = ? AND alert_date BETWEEN ? AND ?
            LIMIT 1
            """,
            (fund_code, alert_type, start_date, end_date),
        ).fetchone()
        return row is not None

    def _current_shares(self, fund_code: str) -> float:
        snapshot = self.build_portfolio_snapshot()
        for item in snapshot["positions"]:
            if item["fund_code"] == fund_code:
                return float(item["shares"])
        return 0.0

    def _plan_matches_market_date(self, plan: dict[str, Any], market_date: date) -> bool:
        start_date = date.fromisoformat(str(plan["start_date"]))
        if market_date < start_date:
            return False
        if plan["end_date"] and market_date > date.fromisoformat(str(plan["end_date"])):
            return False
        if plan["frequency"] == "daily":
            return True
        if plan["frequency"] == "weekly":
            return plan["run_rule"] == WEEKDAY_TO_RULE[market_date.weekday()]
        return False

    def _dca_transaction_exists(self, plan_id: int, trade_date: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM transactions
            WHERE plan_id = ? AND trade_date = ?
            LIMIT 1
            """,
            (plan_id, trade_date),
        ).fetchone()
        return row is not None

    def _threshold_for(self, fund_code: str) -> float:
        row = self.conn.execute(
            "SELECT default_drop_threshold_pct FROM funds WHERE fund_code = ?",
            (fund_code,),
        ).fetchone()
        return float(row["default_drop_threshold_pct"]) if row else self.config.default_drop_threshold_pct

    def _build_alert_message(
        self,
        fund: sqlite3.Row,
        latest_price: sqlite3.Row,
        position: dict[str, Any] | None,
    ) -> str:
        lines = [
            f"基金：{fund['fund_name']} ({fund['fund_code']})",
            f"净值日期：{latest_price['price_date']}",
            f"最新净值：{float(latest_price['nav']):.4f}",
            f"相对前一日跌幅：{float(latest_price['pct_change_vs_prev']):.2f}%",
            f"提醒阈值：-{float(fund['default_drop_threshold_pct']):.2f}%",
        ]
        if position:
            lines.extend(
                [
                    f"当前份额：{position['shares']}",
                    f"当前市值：{position['market_value']}",
                    f"浮动盈亏：{position['unrealized_pnl']}",
                    f"组合占比：{position['weight_pct']}%",
                ]
            )
        lines.append("建议：请结合仓位、预算与原定投计划评估是否补仓。")
        return "\n".join(lines)

    def _send_email(self, subject: str, body: str) -> None:
        email = self.config.email
        if not email.enabled:
            return
        if not all([email.smtp_host, email.username, email.password, email.sender, email.recipient]):
            raise ValueError("邮件配置不完整，请检查 fund_tracker.yaml 和 SMTP 密码环境变量")

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = email.sender
        message["To"] = email.recipient
        message.set_content(body)

        if email.use_ssl:
            with smtplib.SMTP_SSL(email.smtp_host, email.smtp_port, timeout=20) as smtp:
                smtp.login(email.username, email.password)
                smtp.send_message(message)
            return

        with smtplib.SMTP(email.smtp_host, email.smtp_port, timeout=20) as smtp:
            smtp.starttls()
            smtp.login(email.username, email.password)
            smtp.send_message(message)

    def _send_macos_notification(self, title: str, message: str) -> None:
        if not self.config.notifications.macos_enabled:
            return

        safe_title = _escape_applescript_text(title)
        safe_message = _escape_applescript_text(message)
        script = f'display notification "{safe_message}" with title "{safe_title}"'
        subprocess.run(
            ["osascript", "-e", script],
            check=True,
            capture_output=True,
            text=True,
        )


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _escape_applescript_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _latest_point_on_or_before(history: list[PricePoint], target_date: date) -> PricePoint | None:
    candidates = [point for point in history if point.price_date <= target_date]
    return candidates[-1] if candidates else None


def _earliest_point_on_or_after(history: list[PricePoint], target_date: date) -> PricePoint | None:
    for point in history:
        if point.price_date >= target_date:
            return point
    return None


def _next_price_date_after(history: list[PricePoint], target_date: date) -> str:
    for point in history:
        if point.price_date > target_date:
            return point.price_date.isoformat()
    return target_date.isoformat()


def _resolve_confirm_point_with_rule(
    history: list[PricePoint],
    order_at: datetime,
    cutoff_time: str,
    confirm_trade_day_lag: int,
) -> PricePoint | None:
    if not history:
        return None
    base_date = order_at.date()
    cutoff = _parse_hhmm(cutoff_time) or time(15, 0)
    if order_at.time() > cutoff:
        base_date = base_date + timedelta(days=1)

    base_index = None
    for idx, point in enumerate(history):
        if point.price_date >= base_date:
            base_index = idx
            break
    if base_index is None:
        return None
    confirm_index = base_index + max(confirm_trade_day_lag, 0)
    if confirm_index >= len(history):
        return None
    return history[confirm_index]


def _resolve_effective_from_date_with_rule(
    history: list[PricePoint],
    confirm_date: date,
    effective_trade_day_lag_after_confirm: int,
) -> str:
    if not history:
        return confirm_date.isoformat()
    confirm_index = None
    for idx, point in enumerate(history):
        if point.price_date == confirm_date:
            confirm_index = idx
            break
    if confirm_index is None:
        return _add_business_days(confirm_date, max(effective_trade_day_lag_after_confirm, 1)).isoformat()
    effective_index = confirm_index + max(effective_trade_day_lag_after_confirm, 1)
    if effective_index >= len(history):
        return _add_business_days(confirm_date, max(effective_trade_day_lag_after_confirm, 1)).isoformat()
    return history[effective_index].price_date.isoformat()


def _parse_hhmm(text: str) -> time | None:
    matched = re.match(r"^\s*([01]?\d|2[0-3]):([0-5]\d)\s*$", str(text))
    if not matched:
        return None
    return time(hour=int(matched.group(1)), minute=int(matched.group(2)))


def _add_business_days(start: date, days: int) -> date:
    result = start
    remaining = max(days, 0)
    while remaining > 0:
        result = result + timedelta(days=1)
        if result.weekday() < 5:
            remaining -= 1
    return result
