import os
import time
from clickhouse_driver import Client
from functools import lru_cache
import threading

thread_local = threading.local()

RESERVED_KEYWORDS = {
    "INDEX",
    "ENGINE",
    "TABLE",
    "DATABASE",
    "ORDER",
    "BY",
    "PRIMARY",
    "KEY",
    "UNIQUE",
    "PARTITION",
    "TTL",
    "SETTINGS",
    "FORMAT",
    "ALIAS",
    "TTL",
    "SAMPLE",
    "AS",
    "WHERE",
    "HAVING",
    "IN",
    "LIMIT",
    "UNION",
    "ALL",
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "WITH",
    "ALTER",
    "DROP",
    "RENAME",
    "OPTIMIZE",
}


def escape_column_name(column_name):
    if column_name.upper() in RESERVED_KEYWORDS:
        return f"`{column_name}`"
    return column_name


@lru_cache(maxsize=None)
def table_exists(table_name):
    client = get_clickhouse_client()
    query = f"SHOW TABLES LIKE '{table_name}'"
    result = client.execute(query)
    return len(result) > 0


def get_clickhouse_client(retries=10, delay=1):
    if not hasattr(thread_local, "client"):
        clickhouse_host = os.getenv("CLICKHOUSE_HOST")
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        clickhouse_db = os.getenv("CLICKHOUSE_DB")
        clickhouse_user = os.getenv("CLICKHOUSE_USER")
        clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD")

        attempt = 0
        while attempt < retries:
            try:
                thread_local.client = Client(
                    host=clickhouse_host,
                    port=clickhouse_port,
                    user=clickhouse_user,
                    password=clickhouse_password,
                    database=clickhouse_db,
                )
                thread_local.client.execute("SELECT 1")
                break
            except Exception as e:
                print(f"Error connecting to Clickhouse: {e}")
                print(f"Retrying in {delay}s...")
                attempt += 1
                if attempt < retries:
                    time.sleep(delay)
                else:
                    raise e
    return thread_local.client
