from __future__ import annotations

import logging
import os
from datetime import date, datetime
from functools import lru_cache
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.fund_tracker.config import TrackerConfig, load_tracker_config
from src.fund_tracker.database import connect_database
from src.fund_tracker.service import FundTrackerService

app = FastAPI(title="Fund Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@lru_cache(maxsize=1)
def get_tracker_config() -> TrackerConfig:
    return load_tracker_config()


def get_service() -> Iterator[FundTrackerService]:
    config = get_tracker_config()
    conn = connect_database(config.db_path)
    try:
        yield FundTrackerService(conn, config)
    finally:
        conn.close()


class TradeRequest(BaseModel):
    text: str
    trade_date: Optional[str] = None
    order_at: Optional[str] = None


class ReportExecutionActionRequest(BaseModel):
    action_type: str
    sign: Optional[str] = None
    action_label: Optional[str] = None
    fund_code: Optional[str] = None
    fund_name: Optional[str] = None
    amount: Optional[float] = None
    frequency: Optional[str] = None
    run_rule: Optional[str] = None
    note: Optional[str] = None


class ApplyReportExecutionRequest(BaseModel):
    actions: list[ReportExecutionActionRequest]
    trade_date: Optional[str] = None


class IndustryWatchlistRequest(BaseModel):
    themes: list[str]


class BackfillPurchaseFeesRequest(BaseModel):
    overwrite: bool = False


@app.get("/api/summary")
def get_summary(service: FundTrackerService = Depends(get_service)):
    try:
        return service.build_portfolio_snapshot()
    except Exception as exc:
        logging.error("Error building snapshot: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/holdings")
def get_holdings(service: FundTrackerService = Depends(get_service)):
    try:
        snapshot = service.build_portfolio_snapshot()
        return snapshot["positions"]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/transactions")
def get_transactions(limit: int = 50, service: FundTrackerService = Depends(get_service)):
    cursor = service.conn.execute(
        """
        SELECT t.*, f.fund_name
        FROM transactions t
        JOIN funds f ON f.fund_code = t.fund_code
        ORDER BY t.trade_date DESC, t.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


@app.get("/api/funds")
def get_funds(service: FundTrackerService = Depends(get_service)):
    try:
        return service.list_funds()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/analysis-reports")
def get_analysis_reports(limit: int = 20, service: FundTrackerService = Depends(get_service)):
    return service.list_analysis_reports(limit=limit)


@app.get("/api/industry-watchlist")
def get_priority_industry_watchlist(service: FundTrackerService = Depends(get_service)):
    return service.get_priority_industry_watchlist()


@app.put("/api/industry-watchlist")
def update_priority_industry_watchlist(
    req: IndustryWatchlistRequest,
    service: FundTrackerService = Depends(get_service),
):
    try:
        payload = service.update_priority_industry_watchlist(req.themes)
        return {"message": "重点行业标签已更新，下次生成报告时生效。", "payload": payload}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/analysis-reports/generate")
def generate_analysis_report(
    mode: str = "monthly",
    available_cash: Optional[float] = None,
    service: FundTrackerService = Depends(get_service),
):
    try:
        if mode in {"local", "manual"}:
            result = service.apply_text_command("分析当前持仓")
        elif mode == "monthly":
            result = service.generate_external_analysis_report(mode=mode, available_cash=available_cash)
        elif mode == "daily_opportunity":
            result = service.generate_daily_opportunity_report(available_cash=available_cash, notify=False)
        else:
            raise HTTPException(status_code=400, detail="当前仅支持本地分析、增强月报和今日强机会监测。")
        return {"message": result.message, "payload": result.payload}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/analysis-reports/{report_id}/apply")
def apply_analysis_report_execution(
    report_id: int,
    req: ApplyReportExecutionRequest,
    service: FundTrackerService = Depends(get_service),
):
    try:
        trade_date = date.fromisoformat(req.trade_date) if req.trade_date else None
        result = service.apply_analysis_report_execution_plan(
            report_id=report_id,
            actions=[item.model_dump() for item in req.actions],
            trade_date=trade_date,
        )
        return {"message": result.message, "payload": result.payload}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/commands/apply")
def apply_command(req: TradeRequest, service: FundTrackerService = Depends(get_service)):
    try:
        trade_date = date.fromisoformat(req.trade_date) if req.trade_date else None
        order_at = datetime.fromisoformat(req.order_at) if req.order_at else None
        result = service.apply_text_command(req.text, trade_date=trade_date, order_at=order_at)
        return {"message": result.message, "payload": result.payload}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/dca-plans")
def get_dca_plans(service: FundTrackerService = Depends(get_service)):
    try:
        return service.list_dca_plans()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/dca-plans/{fund_code}/pause")
def pause_dca(fund_code: str, service: FundTrackerService = Depends(get_service)):
    try:
        result = service.apply_text_command(f"暂停定投 {fund_code}")
        return {"message": result.message}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/dca-plans/{fund_code}/resume")
def resume_dca(fund_code: str, service: FundTrackerService = Depends(get_service)):
    try:
        result = service.apply_text_command(f"恢复定投 {fund_code}")
        return {"message": result.message}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/dca-plans/{fund_code}/cancel")
def cancel_dca(fund_code: str, service: FundTrackerService = Depends(get_service)):
    try:
        result = service.apply_text_command(f"取消定投 {fund_code}")
        return {"message": result.message}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/tasks/daily-run")
def run_daily_task(service: FundTrackerService = Depends(get_service)):
    try:
        result = service.run_daily()
        return {"message": result.message, "payload": result.payload}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/tasks/test-notification")
def test_notification(service: FundTrackerService = Depends(get_service)):
    try:
        result = service.send_test_notification()
        return {"message": result.message}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/alerts")
def get_alerts(limit: int = 20, service: FundTrackerService = Depends(get_service)):
    cursor = service.conn.execute(
        """
        SELECT a.*, f.fund_name
        FROM alerts a
        JOIN funds f ON f.fund_code = a.fund_code
        ORDER BY a.alert_date DESC, a.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


@app.get("/api/config")
def get_runtime_config(service: FundTrackerService = Depends(get_service)):
    config = service.config
    return {
        "db_path": str(config.db_path),
        "snapshot_dir": str(config.snapshot_dir),
        "notifications": {
            "macos_enabled": config.notifications.macos_enabled,
            "title_prefix": config.notifications.title_prefix,
        },
        "email_enabled": config.email.enabled,
    }


@app.post("/api/tasks/backfill-purchase-fees")
def backfill_purchase_fees(
    req: BackfillPurchaseFeesRequest,
    service: FundTrackerService = Depends(get_service),
):
    try:
        result = service.backfill_purchase_fees(overwrite=bool(req.overwrite))
        return {"message": result.message, "payload": result.payload}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/diagnostics/price-freshness")
def get_price_freshness(date_str: Optional[str] = None, service: FundTrackerService = Depends(get_service)):
    try:
        as_of = date.fromisoformat(date_str) if date_str else None
        return service.get_price_freshness_diagnostics(as_of=as_of)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"日期格式错误：{exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/health")
def get_health():
    return {"status": "ok", "service": "fund-tracker"}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("FUND_TRACKER_API_PORT", "8000"))
    uvicorn.run(app, host="127.0.0.1", port=port)
