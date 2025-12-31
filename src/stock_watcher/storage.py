import duckdb

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