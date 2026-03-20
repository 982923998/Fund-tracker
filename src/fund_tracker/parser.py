from __future__ import annotations

import re
from datetime import date
from dataclasses import dataclass


class CommandParseError(ValueError):
    pass


@dataclass
class ParsedCommand:
    action: str
    payload: dict


def parse_command(text: str) -> ParsedCommand:
    normalized = re.sub(r"\s+", " ", text.strip())
    if not normalized:
        raise CommandParseError("指令为空")
    normalized, explicit_trade_date = _extract_explicit_trade_date(normalized)

    code_first_trade_match = re.match(
        r"^(买入|卖出)\s+(\S+)\s+([0-9]+(?:\.[0-9]+)?)\s*(份)?$",
        normalized,
    )
    if code_first_trade_match:
        trade_type, identifier, amount_text, share_marker = code_first_trade_match.groups()
        if _looks_like_fund_identifier(identifier):
            return ParsedCommand(
                action="trade",
                payload={
                    "trade_type": "buy" if trade_type == "买入" else "sell",
                    "value": float(amount_text),
                    "value_type": "shares" if share_marker else "amount",
                    "identifier": identifier.strip(),
                    "explicit_trade_date": explicit_trade_date,
                },
            )

    trade_match = re.match(
        r"^(买入|卖出)\s+([0-9]+(?:\.[0-9]+)?)\s*(份)?\s+(.+)$",
        normalized,
    )
    if trade_match:
        trade_type, amount_text, share_marker, identifier = trade_match.groups()
        return ParsedCommand(
            action="trade",
            payload={
                "trade_type": "buy" if trade_type == "买入" else "sell",
                "value": float(amount_text),
                "value_type": "shares" if share_marker else "amount",
                "identifier": identifier.strip(),
                "explicit_trade_date": explicit_trade_date,
            },
        )

    dca_match = re.match(
        r"^新增定投\s+([0-9]+(?:\.[0-9]+)?)\s+(每天|每周[一二三四五六日天])\s+(.+)$",
        normalized,
    )
    if dca_match:
        amount_text, frequency_text, identifier = dca_match.groups()
        frequency, run_rule = _parse_frequency(frequency_text)
        return ParsedCommand(
            action="create_dca",
            payload={
                "amount": float(amount_text),
                "frequency": frequency,
                "run_rule": run_rule,
                "identifier": identifier.strip(),
            },
        )

    for action_text, action_name in [
        ("暂停定投", "pause_dca"),
        ("恢复定投", "resume_dca"),
        ("取消定投", "cancel_dca"),
    ]:
        command_match = re.match(rf"^{action_text}\s+(.+)$", normalized)
        if command_match:
            return ParsedCommand(
                action=action_name,
                payload={"identifier": command_match.group(1).strip()},
            )

    if normalized == "查看持仓":
        return ParsedCommand(action="view_holdings", payload={})
    if normalized == "查看收益":
        return ParsedCommand(action="view_performance", payload={})
    if normalized == "分析当前持仓":
        return ParsedCommand(action="analyze_portfolio", payload={})

    raise CommandParseError(f"无法识别指令：{normalized}")


def _parse_frequency(text: str) -> tuple[str, str]:
    if text == "每天":
        return "daily", "daily"

    weekday_mapping = {
        "一": "MON",
        "二": "TUE",
        "三": "WED",
        "四": "THU",
        "五": "FRI",
        "六": "SAT",
        "日": "SUN",
        "天": "SUN",
    }
    weekday = weekday_mapping[text[-1]]
    return "weekly", f"weekly:{weekday}"


def _looks_like_fund_identifier(token: str) -> bool:
    compact = token.strip()
    if not compact:
        return False
    if re.fullmatch(r"\d{6}", compact):
        return True
    return bool(re.search(r"[A-Za-z\u4e00-\u9fff]", compact))


def _extract_explicit_trade_date(text: str) -> tuple[str, str | None]:
    leading = re.match(r"^(?P<date>\d{4}[-/]\d{1,2}[-/]\d{1,2})\s+(?P<rest>.+)$", text)
    if leading:
        parsed = _normalize_date_token(leading.group("date"))
        if parsed is not None:
            return leading.group("rest").strip(), parsed

    trailing = re.match(r"^(?P<rest>.+?)\s+(?P<date>\d{4}[-/]\d{1,2}[-/]\d{1,2})$", text)
    if trailing:
        parsed = _normalize_date_token(trailing.group("date"))
        if parsed is not None:
            return trailing.group("rest").strip(), parsed

    return text, None


def _normalize_date_token(token: str) -> str | None:
    normalized = token.replace("/", "-")
    parts = normalized.split("-")
    if len(parts) != 3:
        return None
    try:
        year = int(parts[0])
        month = int(parts[1])
        day = int(parts[2])
        return date(year, month, day).isoformat()
    except ValueError:
        return None
