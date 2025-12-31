import src.stock_watcher.storage as storage

def main():
    conn = storage.get_connection()
    storage.init_schema(conn)
if __name__ == "__main__":
    main()