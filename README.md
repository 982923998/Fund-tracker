# Fund Tracker

本项目是一个本地运行的基金定投追踪器，适合中国场外基金 / 指数基金场景。

支持功能：

1. 导入初始持仓
2. 记录买入 / 卖出
3. 自动执行日定投 / 周定投
4. 每日抓取基金净值
5. 按相对前一日跌幅发送补仓提醒
6. 生成组合快照与本地分析报告
7. 每日监测是否出现值得打断月报节奏的强机会提醒

## 目录结构

- `fund_tracker_cli.py`：主 CLI 入口
- `src/fund_tracker/`：核心逻辑
- `config/fund_tracker.yaml`：运行配置
- `config/fund_tracker_initial_holdings.example.csv`：初始持仓模板
- `data/`：数据库、快照、日志输出目录
- `run_fund_tracker_daily.sh`：每日任务脚本

## 安装

建议在项目根目录创建虚拟环境：

```bash
cd ~/Desktop/code/fund-tracker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 配置

默认配置文件：

```bash
config/fund_tracker.yaml
```

默认推荐使用 `macOS` 本地通知。配置位于：

```bash
config/fund_tracker.yaml
```

其中：

- `notifications.macos_enabled: true` 表示启用本地通知
- `email.enabled: false` 表示默认不发邮件

如果你仍然需要邮件提醒，再在 `.env` 或当前 shell 中设置：

```bash
export FUND_TRACKER_SMTP_PASSWORD=你的SMTP密码
```

## 初始化数据库

```bash
python3 fund_tracker_cli.py init-db
```

## 导入初始持仓

先复制模板并填写：

```bash
cp config/fund_tracker_initial_holdings.example.csv /tmp/my_holdings.csv
```

导入：

```bash
python3 fund_tracker_cli.py import-initial --csv /tmp/my_holdings.csv
```

## 日常指令

```bash
python3 fund_tracker_cli.py apply --text "买入 1000 110011"
python3 fund_tracker_cli.py apply --text "卖出 500 110011"
python3 fund_tracker_cli.py apply --text "新增定投 1000 每周一 110011"
python3 fund_tracker_cli.py apply --text "新增定投 50 每天 001632"
python3 fund_tracker_cli.py apply --text "暂停定投 110011"
python3 fund_tracker_cli.py summary
python3 fund_tracker_cli.py analyze
python3 fund_tracker_cli.py daily-opportunity --cash 2000
python3 fund_tracker_cli.py test-notification
```

## 每日任务

手动执行：

```bash
python3 fund_tracker_cli.py daily-run
```

或运行：

```bash
./run_fund_tracker_daily.sh
```

如需开机后自动按计划运行，可安装 `launchd` 任务：

```bash
./install_launch_agent.sh
```

说明：

- `launchd` 运行副本会同步到 `~/Library/Application Support/FundTrackerRuntime`
- 自动任务的数据也会写到这个目录下，避免 macOS 对 `Desktop` 后台访问的限制
- 如果你修改了 `config/` 或脚本，重新执行一次 `./install_launch_agent.sh` 即可同步

卸载：

```bash
./uninstall_launch_agent.sh
```

安装后可查看状态：

```bash
launchctl print gui/$(id -u)/com.chenmayao.fund-tracker
```

执行后会更新：

1. `data/fund_tracker.db`
2. `data/fund_tracker_snapshots/latest-snapshot.json`
3. `data/fund_tracker_snapshots/latest-snapshot-for-skill.md`
4. `data/fund_tracker_daily.log`

## 说明

1. 净值数据当前使用东方财富公开 `pingzhongdata` 接口。
2. 第一版收益按移动平均成本法计算。
3. `macOS` 本地通知通过 `osascript` 调用系统通知中心。
4. 这是辅助决策工具，不会自动下单。
