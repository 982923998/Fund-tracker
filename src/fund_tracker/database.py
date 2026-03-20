from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS funds (
        fund_code TEXT PRIMARY KEY,
        fund_name TEXT NOT NULL,
        fund_type TEXT NOT NULL DEFAULT 'fund',
        enabled INTEGER NOT NULL DEFAULT 1,
        default_drop_threshold_pct REAL NOT NULL DEFAULT 1.5,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dca_plans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fund_code TEXT NOT NULL,
        amount REAL NOT NULL,
        frequency TEXT NOT NULL,
        run_rule TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        start_date TEXT NOT NULL,
        end_date TEXT,
        note TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fund_code TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        trade_type TEXT NOT NULL,
        amount REAL NOT NULL,
        nav REAL NOT NULL,
        shares REAL NOT NULL,
        fee REAL NOT NULL DEFAULT 0,
        source TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'posted',
        note TEXT,
        raw_text TEXT,
        plan_id INTEGER,
        created_at TEXT NOT NULL,
        FOREIGN KEY (fund_code) REFERENCES funds(fund_code),
        FOREIGN KEY (plan_id) REFERENCES dca_plans(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_prices (
        fund_code TEXT NOT NULL,
        price_date TEXT NOT NULL,
        nav REAL NOT NULL,
        pct_change_vs_prev REAL,
        source_name TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        PRIMARY KEY (fund_code, price_date),
        FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fund_code TEXT NOT NULL,
        alert_date TEXT NOT NULL,
        alert_type TEXT NOT NULL,
        trigger_value REAL NOT NULL,
        delivery_status TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS analysis_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date TEXT NOT NULL,
        report_type TEXT NOT NULL,
        input_snapshot TEXT NOT NULL,
        skill_name TEXT NOT NULL,
        report_body TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_transactions_fund_date ON transactions(fund_code, trade_date, id)",
    "CREATE INDEX IF NOT EXISTS idx_prices_fund_date ON daily_prices(fund_code, price_date)",
    "CREATE INDEX IF NOT EXISTS idx_alerts_fund_date ON alerts(fund_code, alert_date)",
]


def connect_database(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    with conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        _ensure_column(conn, "funds", "daily_purchase_limit_amount", "REAL")
        _ensure_column(conn, "funds", "purchase_fee_rate_pct", "REAL")
        _ensure_column(conn, "transactions", "order_date", "TEXT")
        _ensure_column(conn, "transactions", "order_at", "TEXT")
        _ensure_column(conn, "transactions", "confirm_nav_date", "TEXT")
        _ensure_column(conn, "transactions", "effective_from_date", "TEXT")


def _ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    definition: str,
) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_columns = {row["name"] for row in rows}
    if column_name in existing_columns:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
