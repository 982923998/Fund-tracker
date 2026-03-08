from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any


class CodexMonthlyBriefingRunner:
    def __init__(self, project_root: Path, runtime_dir: Path | None = None) -> None:
        self.project_root = project_root
        self.runtime_dir = runtime_dir or (project_root / "data" / "codex_briefings")
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.skill_dir = project_root / "skills" / "fund-monthly-briefing"
        self.skill_path = self.skill_dir / "SKILL.md"
        self.cash_plan_skill_dir = project_root / "skills" / "fund-cash-deployment-plan"
        self.cash_plan_skill_path = self.cash_plan_skill_dir / "SKILL.md"
        self.portfolio_advisor_skill_path = project_root / "skills" / "fund-portfolio-advisor" / "SKILL.md"
        self.industry_skill_path = Path.home() / ".codex" / "skills" / "industry-research" / "SKILL.md"
        self.asset_allocation_skill_path = Path.home() / ".agents" / "skills" / "asset-allocation" / "SKILL.md"
        self.output_schema_path = self.skill_dir / "report_output.schema.json"
        self.cash_plan_output_schema_path = self.cash_plan_skill_dir / "report_output.schema.json"
        self.codex_bin = shutil.which("codex")

    def generate_monthly_report(self, material_packet: dict[str, Any]) -> dict[str, Any]:
        if not self.codex_bin:
            raise ValueError("未找到 codex CLI，请先确认本机已安装 Codex。")
        if not self.skill_path.exists():
            raise ValueError(f"月报 skill 不存在：{self.skill_path}")
        if not self.portfolio_advisor_skill_path.exists():
            raise ValueError(f"组合顾问 skill 不存在：{self.portfolio_advisor_skill_path}")
        if not self.output_schema_path.exists():
            raise ValueError(f"月报输出 schema 不存在：{self.output_schema_path}")
        prompt = self._build_prompt(material_packet)
        return self._run_codex_report(
            prompt=prompt,
            material_packet=material_packet,
            output_schema_path=self.output_schema_path,
            prefix="monthly",
            error_label="月报",
        )

    def generate_cash_deployment_plan(self, material_packet: dict[str, Any]) -> str:
        if not self.codex_bin:
            raise ValueError("未找到 codex CLI，请先确认本机已安装 Codex。")
        if not self.cash_plan_skill_path.exists():
            raise ValueError(f"资金方案 skill 不存在：{self.cash_plan_skill_path}")
        if not self.portfolio_advisor_skill_path.exists():
            raise ValueError(f"组合顾问 skill 不存在：{self.portfolio_advisor_skill_path}")
        if not self.cash_plan_output_schema_path.exists():
            raise ValueError(f"资金方案输出 schema 不存在：{self.cash_plan_output_schema_path}")

        prompt = self._build_cash_plan_prompt(material_packet)
        payload = self._run_codex_report(
            prompt=prompt,
            material_packet=material_packet,
            output_schema_path=self.cash_plan_output_schema_path,
            prefix="cash-plan",
            error_label="资金方案",
        )
        return str(payload.get("report_body", "")).strip()

    def _build_prompt(self, material_packet: dict[str, Any]) -> str:
        skill_text = self.skill_path.read_text(encoding="utf-8").strip()
        advisor_skill_text = self.portfolio_advisor_skill_path.read_text(encoding="utf-8").strip()
        industry_skill_text = self._load_industry_skill_text()
        asset_allocation_skill_text = self._load_asset_allocation_skill_text()
        packet_json = json.dumps(material_packet, ensure_ascii=False, indent=2)
        return (
            "你现在是 Fund Tracker 项目的每月投资月报撰写器。\n"
            "你不需要运行任何命令，也不需要读取任何额外文件。\n"
            "请直接依据下面给出的项目私有 skill、组合顾问 skill、行业研究参考 skill、资产配置参考 skill 和材料包，生成最终月报。\n"
            "请严格遵守 skill 的写作规则，并只返回符合 schema 的 JSON。\n"
            "你必须基于材料包里的事实自己做判断，不能把候选基金宇宙或某个字段当作预设答案。\n"
            "候选基金宇宙只是可比较的备选集合，不代表系统已经推荐这些方向；你可以选择其中一部分，也可以明确排除其中某些方向。\n"
            "请先完成组合诊断和资产配置判断，再综合第三到第五部分需要表达的调研结论，写第二部分“配置与操作建议”，最后把第二部分压缩成第一部分“资金调配方案”。\n"
            "如果材料包提供了 available_cash，第一部分必须给出精确到金额的资金调配方案，金额合计必须严格等于 available_cash，并尽量符合 amount_granularity 约束。\n"
            "第一部分必须是今天就能执行的方案：不要建议拆到未来几天，不要建议留现金等待，也不要把钱分配不完。\n"
            "如果材料包提供了 same_day_execution_context、fund_constraints_catalog 或 daily_purchase_limit_amount，你必须把这些当作硬性的操作约束：同一只基金的今日买入金额不能超过它的单日限额；如果今天有定投会执行，要把这部分先计入今日额度占用。\n"
            "这些限购信息是系统从公开基金页面自动同步和维护的，不需要用户手动录入；你应优先信任这些约束，而不是自行假设。\n"
            "如果某只基金因为单日限额无法承接更多金额，就把剩余资金分配给其他今天可执行的已跟踪基金或候选基金，直到 available_cash 全部花完。\n"
            "尤其注意：候选基金也可能出现在 fund_constraints_catalog 中，不能因为它不是当前持仓就忽略它的单日限额。\n"
            "新增资金风格优先靠近 80% 稳健、20% 进攻；但你仍然要结合本月调研结果决定哪些基金更适合承担稳健仓、哪些只适合小比例进攻仓。\n"
            "第一部分涉及具体操作时，必须显式标记动作方向：买入或定投使用“+”，卖出、取消定投或暂停定投使用“-”。\n"
            "第一部分涉及基金时，统一写成“基金名称（基金代码）”格式，不要再写成“代码在前，名称在后”。\n"
            "如果材料包没有提供 available_cash，第一部分也要写成新增资金优先顺序，但标题仍然保持“资金调配方案”。\n"
            "除 report_body 外，你还必须返回 execution_plan 数组，供前端直接编辑和提交执行。\n"
            "execution_plan 必须与 report_body 第一部分“资金调配方案”保持一致，不能互相矛盾。\n"
            "execution_plan 每一项都必须包含：action_type、sign、action_label、fund_code、fund_name；涉及交易金额或定投金额时再填写 amount。\n"
            "action_type 只能使用 buy、sell、create_dca、update_dca、pause_dca、resume_dca、cancel_dca 之一。\n"
            "如果是定投动作，补充 frequency（daily 或 weekly）和 run_rule（如 daily、weekly:MON）。\n"
            "如果第一部分没有定投调整，就不要在 execution_plan 里硬塞定投动作。\n"
            "如果某只现有定投计划只是按原规则继续执行、没有新增或改动，就不要把它写进第一部分资金调配方案，也不要写入 execution_plan；只有在新增、修改、暂停、恢复或取消定投时，才出现定投动作。\n"
            "第二部分必须体现本月宏观、行业和大类资产研究结果对当前配置的含义，不能只重复仓位现状，也不能照抄材料包里的事实句子。\n"
            "第二部分还必须正面讨论当前定投路径：哪些定投适合继续，哪些适合缩减、暂停或转向，以及为什么；但不要机械夸大定投在总配置里的权重。\n"
            "如果建议使用候选基金，必须体现你为何选它、为何没有选别的方向；如果不建议使用，也要明确说明原因。\n"
            "特别注意：涉及当前配置的直接操作建议只能出现在前两部分；后面三部分必须独立描述本月动态和相关行业，不要重复给出配置建议。\n"
            "后面三部分都要尽量补上与候选配置相关的行业观察，但不要再写成‘你应该怎么配’。\n\n"
            "组合顾问 skill 负责帮助你把持仓事实、定投路径、集中度、重叠暴露和候选基金宇宙转成客观判断。\n"
            "行业研究参考 skill 只作为方法论来源，主要吸收其中的行业趋势、关键公司与市场领导者、市场动态、近期发展和未来观察这几类框架。\n"
            "资产配置参考 skill 只作为方法论来源，主要吸收其中的战略配置、分散、再平衡、核心-卫星和风险控制框架。\n"
            "忽略参考 skill 里与 Playwright、截图、写文件、保存 outputs、source attribution 模板相关的工具要求。\n\n"
            "===== PROJECT SKILL START =====\n"
            f"{skill_text}\n"
            "===== PROJECT SKILL END =====\n\n"
            "===== PORTFOLIO ADVISOR SKILL START =====\n"
            f"{advisor_skill_text}\n"
            "===== PORTFOLIO ADVISOR SKILL END =====\n\n"
            "===== INDUSTRY RESEARCH REFERENCE SKILL START =====\n"
            f"{industry_skill_text}\n"
            "===== INDUSTRY RESEARCH REFERENCE SKILL END =====\n\n"
            "===== ASSET ALLOCATION REFERENCE SKILL START =====\n"
            f"{asset_allocation_skill_text}\n"
            "===== ASSET ALLOCATION REFERENCE SKILL END =====\n\n"
            "===== MONTHLY MATERIAL PACKET JSON START =====\n"
            f"{packet_json}\n"
            "===== MONTHLY MATERIAL PACKET JSON END =====\n"
        )

    def _build_cash_plan_prompt(self, material_packet: dict[str, Any]) -> str:
        skill_text = self.cash_plan_skill_path.read_text(encoding="utf-8").strip()
        advisor_skill_text = self.portfolio_advisor_skill_path.read_text(encoding="utf-8").strip()
        asset_allocation_skill_text = self._load_asset_allocation_skill_text()
        packet_json = json.dumps(material_packet, ensure_ascii=False, indent=2)
        return (
            "你现在是 Fund Tracker 项目的本次资金调整方案生成器。\n"
            "你不需要运行任何命令，也不需要读取任何额外文件。\n"
            "请直接依据下面给出的项目私有 skill、组合顾问 skill、资产配置参考 skill 和材料包，生成最终资金调整方案。\n"
            "请严格遵守 skill 的写作规则，并只返回符合 schema 的 JSON。\n"
            "你必须基于材料包里的事实自己做判断，不能把候选基金宇宙或上一份月报当作预设答案。\n"
            "上一份月报只是一份研究背景；你需要结合当前可支配资金、当前持仓、定投路径和组合失衡点，生成这一次真正可执行的金额方案。\n"
            "这次方案必须把 available_cash 全部用完，不允许保留现金，也不要建议拆到未来几天执行。\n"
            "如果材料包里有 same_day_execution_context、fund_constraints_catalog 或 daily_purchase_limit_amount，你必须严格遵守同一基金的单日限额；今天会执行的定投金额也要计入额度占用。\n"
            "这些限购信息是系统从公开基金页面自动同步和维护的，不需要用户手动录入；你应优先信任这些约束，而不是自行假设。\n"
            "如果某只基金今天容量不够，就把剩余金额转给其他今天可执行的基金，直到 available_cash 全部分配完成。\n"
            "候选基金如果出现在 fund_constraints_catalog 中，也必须视为已有明确限购约束，不能忽略。\n"
            "新增资金优先靠近 80% 稳健、20% 进攻，但具体落到哪些基金，仍然要由你基于当前环境和组合问题自主判断。\n"
            "所有具体动作都要显式标记方向：买入或定投用“+”，卖出、取消定投、暂停定投用“-”；基金统一写成“基金名称（基金代码）”。\n"
            "金额必须符合材料包里的 amount_granularity 约束，所有分配项加总必须严格等于 available_cash。\n\n"
            "===== CASH PLAN SKILL START =====\n"
            f"{skill_text}\n"
            "===== CASH PLAN SKILL END =====\n\n"
            "===== PORTFOLIO ADVISOR SKILL START =====\n"
            f"{advisor_skill_text}\n"
            "===== PORTFOLIO ADVISOR SKILL END =====\n\n"
            "===== ASSET ALLOCATION REFERENCE SKILL START =====\n"
            f"{asset_allocation_skill_text}\n"
            "===== ASSET ALLOCATION REFERENCE SKILL END =====\n\n"
            "===== CASH PLAN MATERIAL PACKET JSON START =====\n"
            f"{packet_json}\n"
            "===== CASH PLAN MATERIAL PACKET JSON END =====\n"
        )

    def _run_codex_report(
        self,
        prompt: str,
        material_packet: dict[str, Any],
        output_schema_path: Path,
        prefix: str,
        error_label: str,
    ) -> dict[str, Any]:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        input_path = self.runtime_dir / f"{timestamp}-{prefix}-input.json"
        output_path = self.runtime_dir / f"{timestamp}-{prefix}-output.json"
        prompt_path = self.runtime_dir / f"{timestamp}-{prefix}-prompt.md"
        stdout_path = self.runtime_dir / f"{timestamp}-{prefix}-stdout.log"
        stderr_path = self.runtime_dir / f"{timestamp}-{prefix}-stderr.log"
        report_path = self.runtime_dir / f"{timestamp}-{prefix}-report.txt"

        with open(input_path, "w", encoding="utf-8") as handle:
            json.dump(material_packet, handle, ensure_ascii=False, indent=2)

        prompt_path.write_text(prompt, encoding="utf-8")

        command = [
            self.codex_bin,
            "exec",
            "--skip-git-repo-check",
            "--ephemeral",
            "--color",
            "never",
            "--sandbox",
            "read-only",
            "-C",
            str(self.project_root),
            "--output-schema",
            str(output_schema_path),
            "-o",
            str(output_path),
            "-",
        ]
        result = subprocess.run(
            command,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=900,
            check=False,
        )

        stdout_path.write_text(result.stdout or "", encoding="utf-8")
        stderr_path.write_text(result.stderr or "", encoding="utf-8")

        if result.returncode != 0:
            message = (result.stderr or result.stdout or "").strip()
            raise ValueError(f"Codex {error_label}生成失败：{message[-800:]}")
        if not output_path.exists():
            raise ValueError(f"Codex {error_label}生成失败：没有拿到结构化输出文件。")

        raw_output = output_path.read_text(encoding="utf-8").strip()
        if not raw_output:
            raise ValueError(f"Codex {error_label}生成失败：输出为空。")

        payload = self._parse_output(raw_output)
        report_body = str(payload.get("report_body", "")).strip()
        if not report_body:
            raise ValueError(f"Codex {error_label}生成失败：未解析出 report_body。")

        report_path.write_text(report_body, encoding="utf-8")
        return payload

    def _load_industry_skill_text(self) -> str:
        if not self.industry_skill_path.exists():
            return (
                "Industry research reference unavailable. "
                "Fallback methodology: discuss industry trends, market leaders, market dynamics, "
                "recent developments, and near-term watchpoints for each of the last three sections."
            )
        return self.industry_skill_path.read_text(encoding="utf-8").strip()

    def _load_asset_allocation_skill_text(self) -> str:
        if not self.asset_allocation_skill_path.exists():
            return (
                "Asset allocation reference unavailable. "
                "Fallback methodology: prioritize diversification, concentration control, "
                "core-satellite thinking, and long-term rebalancing discipline."
            )
        return self.asset_allocation_skill_path.read_text(encoding="utf-8").strip()

    def _parse_output(self, raw_output: str) -> dict[str, Any]:
        try:
            payload = json.loads(raw_output)
        except json.JSONDecodeError:
            return {"report_body": raw_output.strip(), "execution_plan": []}

        if not isinstance(payload, dict):
            return {"report_body": str(payload).strip(), "execution_plan": []}

        report_body = str(payload.get("report_body", "")).strip()
        execution_plan = payload.get("execution_plan")
        if not isinstance(execution_plan, list):
            execution_plan = []
        execution_plan = [item for item in execution_plan if isinstance(item, dict)]
        return {
            "report_body": report_body,
            "execution_plan": execution_plan,
        }
