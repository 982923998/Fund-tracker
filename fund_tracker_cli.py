#!/usr/bin/env python3
"""
基金定投追踪 CLI
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.fund_tracker.config import load_tracker_config
from src.fund_tracker.database import connect_database, ensure_schema
from src.fund_tracker.service import FundTrackerService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基金定投追踪 CLI")
    parser.add_argument(
        "--config",
        help="fund_tracker 配置文件路径，默认 config/fund_tracker.yaml",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="初始化数据库")

    import_parser = subparsers.add_parser("import-initial", help="导入初始持仓 CSV")
    import_parser.add_argument("--csv", required=True, help="CSV 文件路径")

    apply_parser = subparsers.add_parser("apply", help="执行自然语言指令")
    apply_parser.add_argument("--text", required=True, help="自然语言指令文本")
    apply_parser.add_argument("--date", help="交易日期 YYYY-MM-DD，默认今天")

    daily_parser = subparsers.add_parser("daily-run", help="执行每日任务")
    daily_parser.add_argument("--date", help="运行日期 YYYY-MM-DD，默认今天")

    summary_parser = subparsers.add_parser("summary", help="查看当前组合")
    summary_parser.add_argument("--json", action="store_true", help="输出 JSON")

    analyze_parser = subparsers.add_parser("analyze", help="生成当前持仓分析")
    analyze_parser.add_argument("--json", action="store_true", help="输出 JSON")

    subparsers.add_parser("test-notification", help="发送测试通知")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config = load_tracker_config(args.config)
    conn = connect_database(config.db_path)
    ensure_schema(conn)
    service = FundTrackerService(conn, config)

    try:
        if args.command == "init-db":
            print(f"数据库已初始化：{config.db_path}")
            return 0

        if args.command == "import-initial":
            result = service.import_initial_holdings(Path(args.csv).expanduser().resolve())
            _print_result(result.message, result.payload)
            return 0

        if args.command == "apply":
            command_date = date.fromisoformat(args.date) if args.date else None
            result = service.apply_text_command(args.text, trade_date=command_date)
            _print_result(result.message, result.payload)
            return 0

        if args.command == "daily-run":
            run_date = date.fromisoformat(args.date) if args.date else None
            result = service.run_daily(run_date=run_date)
            _print_result(result.message, result.payload)
            return 0

        if args.command == "summary":
            snapshot = service.build_portfolio_snapshot()
            service.save_snapshot(snapshot)
            if args.json:
                print(json.dumps(snapshot, ensure_ascii=False, indent=2))
            else:
                print(_format_snapshot(snapshot))
            return 0

        if args.command == "analyze":
            result = service.apply_text_command("分析当前持仓")
            if args.json:
                print(json.dumps(result.payload, ensure_ascii=False, indent=2))
            else:
                print(result.payload["report"])
            return 0

        if args.command == "test-notification":
            result = service.send_test_notification()
            _print_result(result.message, result.payload)
            return 0
    except Exception as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 1


def _print_result(message: str, payload: dict) -> None:
    print(message)
    if payload:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


def _format_snapshot(snapshot: dict) -> str:
    lines = [
        "# 当前组合",
        f"- 生成时间：{snapshot['generated_at']}",
        f"- 持仓数量：{snapshot['portfolio']['position_count']}",
        f"- 总市值：{snapshot['portfolio']['total_market_value']:.2f}",
        f"- 浮动盈亏：{snapshot['portfolio']['total_unrealized_pnl']:.2f}",
        f"- 已实现盈亏：{snapshot['portfolio']['total_realized_pnl']:.2f}",
        "",
        "## 持仓明细",
    ]
    for item in snapshot["positions"]:
        lines.append(
            "- "
            f"{item['fund_code']} {item['fund_name']} | 份额 {item['shares']} | "
            f"市值 {item['market_value']:.2f} | 收益率 {item['return_pct']:.2f}% | "
            f"当日涨跌 {item['daily_pct_change'] if item['daily_pct_change'] is not None else 'N/A'}%"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
