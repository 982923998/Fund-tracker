# AI Portfolio Advisor Design

## Goal

把当前月报链路从“规则决定推荐方向，AI负责润色”改成“代码只提供事实，AI独立完成组合判断与候选筛选”。

## Current Problem

- `external_research.py` 里使用固定阈值决定推荐主题
- `candidate_pool` 实际上已经内含方向判断
- Codex 月报更多是在把既定方向写成报告，而不是独立分析

## New Design

### Data Layer

- 保留持仓、定投、市场资讯抓取
- 新增 `portfolio_diagnostics`
  - 集中度
  - 地域暴露
  - 基准族重叠
  - 定投路径分布
- 把 `candidate_pool` 改成 `candidate_universe`
  - 只提供可比较基金集合
  - 不再携带系统先验推荐

### Skill Layer

- 保留 `fund-monthly-briefing`
  - 负责月报结构和写作约束
- 新增 `fund-portfolio-advisor`
  - 负责组合诊断、环境匹配、候选筛选方法
- 同时加载外部参考：
  - `industry-research`
  - `asset-allocation`

### Prompt Layer

- 明确要求 AI 先做组合诊断，再写配置建议
- 明确要求 AI 不得把 `candidate_universe` 当成默认推荐
- 明确要求 AI 对推荐与排除都给出理由

## Expected Outcome

- 推荐方向不再由 Python 阈值规则拍板
- 月报第一、二部分真正体现 AI 对持仓与环境的综合判断
- 候选基金变成“AI筛选后的结果”，而不是“系统先定好方向再让 AI 描述”
