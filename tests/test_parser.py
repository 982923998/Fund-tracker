from __future__ import annotations

import unittest

from src.fund_tracker.parser import parse_command


class ParserTests(unittest.TestCase):
    def test_trade_supports_code_first_amount(self) -> None:
        parsed = parse_command("买入 161125 10")
        self.assertEqual(parsed.action, "trade")
        self.assertEqual(parsed.payload["trade_type"], "buy")
        self.assertEqual(parsed.payload["identifier"], "161125")
        self.assertEqual(parsed.payload["value"], 10.0)
        self.assertEqual(parsed.payload["value_type"], "amount")

    def test_trade_keeps_amount_first_for_numeric_amount(self) -> None:
        parsed = parse_command("买入 100 161125")
        self.assertEqual(parsed.action, "trade")
        self.assertEqual(parsed.payload["trade_type"], "buy")
        self.assertEqual(parsed.payload["identifier"], "161125")
        self.assertEqual(parsed.payload["value"], 100.0)
        self.assertEqual(parsed.payload["value_type"], "amount")

    def test_trade_supports_trailing_explicit_date(self) -> None:
        parsed = parse_command("买入 006479 10 2026-03-12")
        self.assertEqual(parsed.action, "trade")
        self.assertEqual(parsed.payload["identifier"], "006479")
        self.assertEqual(parsed.payload["value"], 10.0)
        self.assertEqual(parsed.payload["explicit_trade_date"], "2026-03-12")

    def test_trade_supports_leading_explicit_date(self) -> None:
        parsed = parse_command("2026/03/12 买入 006479 10")
        self.assertEqual(parsed.action, "trade")
        self.assertEqual(parsed.payload["identifier"], "006479")
        self.assertEqual(parsed.payload["value"], 10.0)
        self.assertEqual(parsed.payload["explicit_trade_date"], "2026-03-12")


if __name__ == "__main__":
    unittest.main()
