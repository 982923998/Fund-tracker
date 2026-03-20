from __future__ import annotations

import json
import math
import re
import unittest
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SKILL_DIR = PROJECT_ROOT / "skills" / "fund-daily-opportunity-monitor"
SKILL_PATH = SKILL_DIR / "SKILL.md"
SCHEMA_PATH = SKILL_DIR / "report_output.schema.json"


def assert_matches_schema(schema: dict, value: object, path: str = "root") -> None:
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        matched = False
        errors: list[str] = []
        for candidate_type in schema_type:
            try:
                assert_matches_schema({**schema, "type": candidate_type}, value, path)
            except AssertionError as exc:
                errors.append(str(exc))
            else:
                matched = True
                break
        if not matched:
            raise AssertionError("; ".join(errors))
        return

    if "const" in schema:
        if value != schema["const"]:
            raise AssertionError(f"{path}: expected const {schema['const']!r}, got {value!r}")

    if "enum" in schema:
        if value not in schema["enum"]:
            raise AssertionError(f"{path}: expected one of {schema['enum']!r}, got {value!r}")

    if schema_type == "object":
        if not isinstance(value, dict):
            raise AssertionError(f"{path}: expected object, got {type(value).__name__}")
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                raise AssertionError(f"{path}: missing required property {key!r}")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extras = set(value) - set(properties)
            if extras:
                raise AssertionError(f"{path}: unexpected properties {sorted(extras)!r}")
        for key, child_schema in properties.items():
            if key in value:
                assert_matches_schema(child_schema, value[key], f"{path}.{key}")
        return

    if schema_type == "array":
        if not isinstance(value, list):
            raise AssertionError(f"{path}: expected array, got {type(value).__name__}")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            raise AssertionError(f"{path}: expected at most {schema['maxItems']} items, got {len(value)}")
        if "minItems" in schema and len(value) < schema["minItems"]:
            raise AssertionError(f"{path}: expected at least {schema['minItems']} items, got {len(value)}")
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                assert_matches_schema(item_schema, item, f"{path}[{index}]")
        return

    if schema_type == "string":
        if not isinstance(value, str):
            raise AssertionError(f"{path}: expected string, got {type(value).__name__}")
        if "minLength" in schema and len(value) < schema["minLength"]:
            raise AssertionError(f"{path}: expected minLength {schema['minLength']}, got {len(value)}")
        if "pattern" in schema and re.fullmatch(schema["pattern"], value) is None:
            raise AssertionError(f"{path}: value {value!r} does not match pattern {schema['pattern']!r}")
        return

    if schema_type == "boolean":
        if not isinstance(value, bool):
            raise AssertionError(f"{path}: expected boolean, got {type(value).__name__}")
        return

    if schema_type == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise AssertionError(f"{path}: expected number, got {type(value).__name__}")
        if "minimum" in schema and value < schema["minimum"]:
            raise AssertionError(f"{path}: expected minimum {schema['minimum']}, got {value}")
        if "multipleOf" in schema:
            ratio = value / schema["multipleOf"]
            if not math.isclose(ratio, round(ratio), rel_tol=0.0, abs_tol=1e-9):
                raise AssertionError(f"{path}: expected multipleOf {schema['multipleOf']}, got {value}")
        return

    if schema_type == "null":
        if value is not None:
            raise AssertionError(f"{path}: expected null, got {value!r}")
        return

    raise AssertionError(f"{path}: unsupported schema type {schema_type!r}")


class DailyOpportunitySkillTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.skill_text = SKILL_PATH.read_text(encoding="utf-8")
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    def test_skill_frontmatter_is_minimal_and_references_support_docs(self) -> None:
        parts = self.skill_text.split("---", 2)
        self.assertGreaterEqual(len(parts), 3)
        frontmatter = yaml.safe_load(parts[1])
        self.assertEqual(set(frontmatter.keys()), {"name", "description"})
        self.assertEqual(frontmatter["name"], "fund-daily-opportunity-monitor")
        self.assertIn("watch / consider / strong_buy", frontmatter["description"])
        self.assertIn("references/decision-rubric.md", self.skill_text)
        self.assertIn("references/material-packet-fields.md", self.skill_text)
        self.assertIn("priority_industry_watch_snapshot", self.skill_text)
        self.assertIn("重点行业速览", self.skill_text)
        self.assertIn("代表基金今日情况", self.skill_text)
        self.assertIn("不能跳过行业", self.skill_text)
        self.assertIn("默认列出 3 只代表基金", self.skill_text)
        self.assertIn("当前仅找到 X 只代表基金", self.skill_text)

    def test_watch_payload_matches_schema(self) -> None:
        payload = {
            "report_body": "今日结论\n今天没有达到需要立刻买入的强信号。\n无动作原因\n当前更像观察窗口，而不是例外买点。\n风险与执行提醒\n继续跟踪国内宽基与防御资产的后续线索。",
            "recommendation_level": "watch",
            "should_alert": False,
            "summary": "今天暂不建议打断月报节奏。",
            "no_action_reason": "当前市场线索不足以支持今天立刻加仓，且组合层面没有出现必须当天修正的缺口。",
            "opportunities": [],
            "expires_at": None,
        }

        assert_matches_schema(self.schema, payload)
        self.assertFalse(payload["should_alert"])
        self.assertEqual(payload["opportunities"], [])
        self.assertIsNotNone(payload["no_action_reason"])

    def test_strong_buy_payload_matches_schema(self) -> None:
        payload = {
            "report_body": "今日结论\n今天存在一笔可以立即执行的例外买点。\n强机会\n- + 国金中证A500指数增强A（022485）：1000元\n风险与执行提醒\n执行后仍需接受短期波动，不要把这笔买入理解成短线抄底。",
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
                    "thesis": "当前组合对国内核心权益配置偏弱，这只基金更适合作为补核心仓位的入口。",
                    "why_now": "近期回撤后估值与情绪压力有所释放，同时国内政策与资金线索对核心宽基更友好。",
                    "portfolio_fit": "补足当前组合对国内核心权益的缺口，降低对单一海外主线的依赖。",
                    "constraint_check": {
                        "purchase_status": "开放申购",
                        "daily_purchase_limit_amount": 5000,
                        "today_due_dca_amount": 0,
                        "today_remaining_purchase_capacity": 5000,
                        "same_day_executable": True,
                    },
                    "risks": [
                        "如果后续政策与资金线索弱于预期，修复节奏可能拉长。",
                        "当前仍处于波动期，短期净值可能继续震荡。",
                    ],
                }
            ],
            "expires_at": "2026-03-09T23:59:59+08:00",
        }

        assert_matches_schema(self.schema, payload)
        self.assertTrue(payload["should_alert"])
        self.assertGreater(len(payload["opportunities"]), 0)
        self.assertIsNone(payload["no_action_reason"])

    def test_invalid_payload_is_rejected(self) -> None:
        payload = {
            "report_body": "坏样例",
            "recommendation_level": "buy_now",
            "should_alert": True,
            "summary": "坏样例",
            "no_action_reason": None,
            "opportunities": [
                {
                    "fund_code": "ABC",
                    "fund_name": "错误基金",
                    "action_type": "sell",
                    "suggested_amount": 555,
                    "thesis": "坏样例",
                    "why_now": "坏样例",
                    "portfolio_fit": "坏样例",
                    "constraint_check": {
                        "purchase_status": "开放申购",
                        "daily_purchase_limit_amount": 1000,
                        "today_due_dca_amount": 0,
                        "today_remaining_purchase_capacity": 1000,
                        "same_day_executable": False,
                    },
                    "risks": ["坏样例"],
                }
            ],
            "expires_at": "2026-03-09T23:59:59+08:00",
        }

        with self.assertRaises(AssertionError):
            assert_matches_schema(self.schema, payload)


if __name__ == "__main__":
    unittest.main()
