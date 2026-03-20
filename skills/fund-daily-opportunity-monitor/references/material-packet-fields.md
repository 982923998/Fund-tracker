# Material Packet Fields

## Core Decision Fields

- `portfolio_snapshot`
  - 当前组合事实总表
  - 优先读取 `positions`、`active_dca_plans`、`same_day_execution_context`
- `holdings_summary`
  - 持仓的中性摘要
  - 用来快速判断当前主要暴露
- `portfolio_diagnostics`
  - 集中度、地域暴露、基准族重叠、定投路径分布
  - 这是事实，不是推荐结论
- `candidate_universe`
  - 系统从全市场基金扫描后压缩出的重点候选摘要
  - 只表示可看，不表示默认推荐
- `candidate_universe_scope`
  - 候选摘要的扫描范围与生成方式
  - 如果存在，默认理解为这次不是只看一小撮手工候选
- `priority_industry_watchlist`
  - 当前系统重点关注的行业方向
  - 用来提高关注权重，不是自动推荐名单
- `priority_industry_watch_snapshot`
  - 重点行业当天的压缩观察快照
  - 包含每个行业的 `today_summary` 和 `representative_funds`
  - `representative_funds` 默认按每个行业最多 3 只代表基金准备
  - 每日机会判断和日报正文应优先复用它

## Constraint Fields

- `same_day_execution_context`
  - 当天执行约束总表
  - 重点看是否有今日定投、剩余额度和是否超限
- `fund_constraints_catalog`
  - 已同步的基金限购目录
  - 候选基金也可能出现在这里，不能忽略
- `allocation_constraints`
  - 金额粒度、是否必须当天执行、是否必须用完现金等约束
- `available_cash`
  - 可选字段
  - 如果不存在，可以不给精确金额

## Context Fields

- `market_context`
  - 对国内、海外、商品和大类资产的背景摘要
  - 用于解释“为什么是今天”
- `news_events`
  - 去重后的近期事件列表
  - 优先关注更近、可信度更高、与候选方向直接相关的事件
- `latest_monthly_report`
  - 最近的月报结论
  - 作为背景约束使用，不要机械照抄

## Missing Data Rules

- 字段缺失时，不要臆造
- 没有 `available_cash` 时，允许 `suggested_amount = null`
- 没有足够事件支持时，宁可输出 `watch`
- 不能证明今天更值得行动时，不要输出 `strong_buy`
