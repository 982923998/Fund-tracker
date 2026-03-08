# Cash Deployment Plan Design

## Goal

支持用户在网页中输入当前一次性可支配资金，并生成精确到金额的本次调整方案。

## User Flow

1. 用户在投资建议页输入当前可支配资金，默认 2000
2. 点击“生成本次调整方案”
3. 后端读取当前持仓、定投、组合诊断、候选基金宇宙和最近一份月报
4. Codex 基于私有资金方案 skill 生成本次执行方案
5. 结果写入 `analysis_reports`，按历史保留

## Design

### Backend

- 新增 `generate_cash_deployment_plan`
- 新增 `build_cash_deployment_material_packet`
- 复用 `analysis_reports`，新增 `report_type = external_cash_plan`

### AI Layer

- 新增 `fund-cash-deployment-plan` skill
- 组合顾问 skill 继续负责诊断与筛选方法
- 输出必须包含：
  - 本次直接执行方案
  - 具体金额分配
  - 分配逻辑与依据
  - 暂不分配方向

### Frontend

- 在投资建议页增加可支配资金输入框
- 默认值 `2000`
- 增加“生成本次调整方案”按钮
- 资金方案以独立卡片形式展示，并保留历史
