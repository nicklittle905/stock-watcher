import duckdb
import json
from typing import Optional

from src.stock_watcher.models import ProductState  # adjust import path to your layout

def get_connection(db_path: str = "stock_watcher.duckdb"):
    """
    Opens (or creates) a DuckDB database file and returns a connection.
    """
    conn = duckdb.connect(db_path)
    return conn

def init_schema(conn) -> None:
    """Create raw_product_checks if it doesn't exist."""
    sql = """
        CREATE TABLE IF NOT EXISTS raw_product_checks (
            vendor TEXT,
            url TEXT,
            product_name TEXT,
            price DECIMAL(10, 2),
            currency TEXT,
            in_stock BOOLEAN,
            checked_at TIMESTAMP,
            raw_meta JSON
        );
    """
    
    conn.execute(sql)

def insert_product_state(conn, state: ProductState) -> None:
    sql = """
        INSERT INTO raw_product_checks (
            vendor,
            url,
            product_name,
            price,
            currency,
            in_stock,
            checked_at,
            raw_meta
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    params = (
        state.vendor,
        state.url,
        state.product_name,
        state.price,          # Decimal is fine; DuckDB will map it to NUMERIC
        state.currency,
        state.in_stock,
        state.checked_at,
        json.dumps(state.raw_meta) if state.raw_meta is not None else None,
    )

    conn.execute(sql, params)

def get_last_product_state(conn, vendor: str, url: str) -> Optional[ProductState]:
    sql = """
        SELECT
            vendor,
            url,
            product_name,
            price,
            currency,
            in_stock,
            checked_at,
            raw_meta
        FROM raw_product_checks
        WHERE vendor = ?
          AND url = ?
        ORDER BY checked_at DESC
        LIMIT 1
    """

    row = conn.execute(sql, (vendor, url)).fetchone()

    if row is None:
        return None

    (
        vendor_val,
        url_val,
        product_name,
        price,
        currency,
        in_stock,
        checked_at,
        raw_meta_json,
    ) = row

    raw_meta = json.loads(raw_meta_json) if raw_meta_json is not None else {}

    return ProductState(
        vendor=vendor_val,
        url=url_val,
        product_name=product_name,
        price=price,
        currency=currency,
        in_stock=in_stock,
        checked_at=checked_at,
        raw_meta=raw_meta,
    )
