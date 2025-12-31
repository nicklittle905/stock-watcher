from datetime import datetime
from decimal import Decimal

from src.stock_watcher.models import ProductState
from src.stock_watcher import storage

def main():
    conn = storage.get_connection()
    storage.init_schema(conn)

    state = ProductState(
        vendor="decathlon",
        url="https://example.com",
        product_name="Test Product",
        price=Decimal("99.99"),
        currency="GBP",
        in_stock=True,
        checked_at=datetime.utcnow(),
    )

    storage.insert_product_state(conn, state)

    last = storage.get_last_product_state(conn, "decathlon", "https://example.com")
    print(last)
    
if __name__ == "__main__":
    main()