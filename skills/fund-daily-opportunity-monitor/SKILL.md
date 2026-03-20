---
name: fund-daily-opportunity-monitor
description: 为 Fund Tracker 项目监测每日市场与基金动态，并判断是否出现足够强的当日例外买入机会。适用于用户以月报作为主线配置，但希望系统每天结合当前持仓、全市场基金扫描后的重点候选摘要、新闻事件、基金单日限购和当日执行上下文，输出 watch / consider / strong_buy 级别的结构化机会判断与提醒。
---

# Fund Tracker 私有每日强机会监测技能

这个技能只服务 `fund-tracker` 项目，不是通用日报生成器。

## 任务目标

回答一个单一问题：

- 今天是否值得打断月报节奏，给出一笔当日就能执行的买入建议？

默认立场是“不动作”。只有在信号足够强、组合适配明确、并且今天可执行时，才输出高优先级提醒。

## 输入材料

运行时通常会给你一个 JSON 材料包，常见字段包括：

- `report_date`
- `investor_profile`
- `available_cash`
- `allocation_constraints`
- `same_day_execution_context`
- `fund_constraints_catalog`
- `portfolio_snapshot`
- `holdings_summary`
- `portfolio_diagnostics`
- `market_context`
- `candidate_universe`
- `candidate_universe_scope`
- `priority_industry_watchlist`
- `priority_industry_watch_snapshot`
- `news_events`
- `latest_monthly_report`

其中：

- `available_cash` 可能不存在；没有这笔字段时，允许 `suggested_amount` 为 `null`
- `same_day_execution_context` 和 `fund_constraints_catalog` 是硬约束，不是建议
- `candidate_universe` 是系统从全市场基金扫描后压缩出的重点候选摘要，不代表默认推荐
- `candidate_universe_scope` 用来说明扫描范围；如果它存在，默认理解为这次不是只看一小撮手工候选
- `priority_industry_watchlist` 表示当前应重点关注的行业方向，但仍然不是自动推荐
- `priority_industry_watch_snapshot` 会给出重点行业当天的大概情况和代表基金，日报里应优先复用它，而不是自己跳过这些行业
- `latest_monthly_report` 是背景，不是必须继承的结论
- `news_events` 是你判断“为什么是今天”的关键依据之一，但不能单独决定买入

如果字段含义不清晰，再读：

- `references/material-packet-fields.md`
- `references/decision-rubric.md`

## 判断顺序

1. 先检查硬约束
2. 再判断今天是否真的出现了“例外级别”的买点
3. 再判断它对当前组合是否有补位价值
4. 最后才决定等级、提醒与建议金额

具体执行时：

1. 先淘汰今天不能执行的方向：
   - 暂停申购
   - 今日剩余额度不足
   - 今天已有定投占满额度
   - 金额粒度无法满足
2. 再判断信号是否足够强：
   - 是否有明确的“为什么是今天”
   - 是否存在事件、政策、市场或估值线索支持
   - 是否只是短期热度，而不是更有利的中期切入点
3. 再判断组合适配：
   - 是补核心仓、防御仓、对冲仓，还是只是重复暴露
   - 是否与月报主线冲突
   - 是否会继续放大组合已经过高的集中度
4. 只在理由能同时回溯到“市场线索 + 组合含义 + 今日可执行”时，才升级到 `strong_buy`

## 等级校准

- `watch`
  - 仅表示值得继续观察
  - 不足以触发买入提醒
  - 默认 `should_alert = false`
- `consider`
  - 有一定吸引力，但缺少一个关键条件
  - 可以写进日内观察结论，但默认不推送强提醒
- `strong_buy`
  - 明确值得今天破例行动
  - 必须同时满足：理由强、组合适配清楚、今天能执行、风险可说明
  - 默认 `should_alert = true`

遇到边界案例时，优先按 `references/decision-rubric.md` 校准，不要自行放松标准。

## 金额建议规则

1. 没有 `available_cash` 时，允许 `suggested_amount = null`
2. 有 `available_cash` 时，金额必须符合 `amount_granularity`
3. 金额不能超过今日剩余额度，也不能无视今日定投占用
4. 单次最多给出 2 只基金，避免把每日强机会写成分散清单
5. 如果你只能说“值得关注”，但说不出今天该买多少，就不要冒充 `strong_buy`

## 输出要求

只返回符合 schema 的 JSON。顶层字段必须包含：

- `report_body`
- `recommendation_level`
- `should_alert`
- `summary`
- `no_action_reason`
- `opportunities`
- `expires_at`

其中：

- `report_body` 使用自然语言中文，优先写成短区块，长度控制在“1 到 2 分钟内能读完”的范围
- `summary` 是一句话结论
- `opportunities` 最多 2 项
- `opportunities` 中的 `action_type` 只能是 `buy`
- 如果 `recommendation_level = watch`，通常应当没有 `opportunities`
- 如果 `recommendation_level = strong_buy`，必须给出至少 1 个 `opportunities`
- 如果没有动作，`no_action_reason` 必须明确写出为什么今天不该买
- `expires_at` 用来表示这份判断何时失效；当天机会默认应在当天收盘前失效
- `report_body` 建议固定包含以下小标题：
  - `今日结论`
  - `重点行业速览`
  - `代表基金今日情况`
  - `风险与执行提醒`
- 如果提供了 `priority_industry_watch_snapshot`，`重点行业速览` 里必须按快照顺序把每个重点行业逐个写一句今天的大概情况，不能跳过行业
- `代表基金今日情况` 必须按同样顺序覆盖 `priority_industry_watch_snapshot` 里的每个重点行业
- 对每个重点行业，默认列出 3 只代表基金；若材料里不足 3 只，也要显式写明“当前仅找到 X 只代表基金”
- 每只代表基金至少写清楚以下信息中的两项：今日涨跌、近1周或近1月表现、申购状态/单日额度等执行约束

## 写作规则

- 用直接、短句、中文表达，不要写成长报告
- 要明确回答“为什么是今天”，不要只说“长期看好”
- 优先写对当前组合的配置意义，而不是泛泛点评市场
- 如果推荐候选基金，必须点明它为何优于当前更热门但重复的方向
- 如果今天不动作，也要把原因写清楚，避免空泛地说“继续观察”

## 禁止事项

- 不要把普通回撤包装成 `strong_buy`
- 不要因为短期热度或媒体标题就给高等级建议
- 不要推荐今天无法执行的基金
- 不要忽略单日限购、今日定投占用或金额粒度
- 不要把与当前持仓高度重复的暴露说成“分散”
- 不要输出免责声明，不要输出来源列表，不要输出 Markdown 代码块
