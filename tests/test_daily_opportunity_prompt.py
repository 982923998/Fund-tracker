from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.fund_tracker.codex_briefing import CodexMonthlyBriefingRunner


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DailyOpportunityPromptTests(unittest.TestCase):
    def test_prompt_requires_every_priority_industry_to_list_three_funds(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runner = CodexMonthlyBriefingRunner(PROJECT_ROOT, runtime_dir=Path(tempdir))
            prompt = runner._build_daily_opportunity_prompt(
                {
                    "report_date": "2026-03-09",
                    "priority_industry_watch_snapshot": [
                        {
                            "theme": "半导体",
                            "today_summary": "板块偏强。",
                            "representative_funds": [],
                        }
                    ],
                }
            )

        self.assertIn("按快照里的每个重点行业逐个写一句今天的大概情况", prompt)
        self.assertIn("每个行业默认列出 3 只代表基金", prompt)
        self.assertIn("不足 3 只，也要明确写出不足", prompt)


if __name__ == "__main__":
    unittest.main()
