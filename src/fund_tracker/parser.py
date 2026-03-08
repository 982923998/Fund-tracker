from __future__ import annotations

import re
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

