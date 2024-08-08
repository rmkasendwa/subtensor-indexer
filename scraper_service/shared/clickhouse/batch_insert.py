import threading
from time import sleep
from shared.clickhouse.utils import get_clickhouse_client


buffer = {}
buffer_lock = threading.Lock()


def batch_insert_into_clickhouse_table(table, rows):
    try:
        formatted_rows = ", ".join(
            f"({','.join(str(value) for value in row)})" for row in rows
        )
        sql = f"INSERT INTO {
            table} SETTINGS async_insert=1, wait_for_async_insert=1 VALUES {formatted_rows}"
        get_clickhouse_client().execute(sql)
    except Exception as e:
        if len(rows) > 1:
            mid = len(rows) // 2
            print(
                f"Error inserting into {table}: {
                    e}. Retrying with smaller batches..."
            )
            batch_insert_into_clickhouse_table(table, rows[:mid])
            batch_insert_into_clickhouse_table(table, rows[mid:])
        else:
            print(f"Error inserting single row into {table}: {e}")
            formatted_rows = ", ".join(
                f"({','.join(str(value) for value in row)})" for row in rows
            )
            sql = f"INSERT INTO {
                table} SETTINGS async_insert=1, wait_for_async_insert=1 VALUES {formatted_rows}"
            print(sql)
            raise e


def buffer_insert(table_name, row):
    """
    Queues a row for insertion. This should be the only way data is inserted into Clickhouse.
    """
    global buffer
    with buffer_lock:
        if table_name not in buffer:
            buffer[table_name] = []
        buffer[table_name].append(row)


# Continuously flush the buffer
def flush_buffer(executor, started_cb, done_cb):
    """
    Continuously flush the buffer.
    """
    global buffer
    while True:
        started_cb()
        with buffer_lock:
            tasks = [(table_name, rows) for table_name, rows in buffer.items()]
            buffer.clear()

        futures = [
            executor.submit(batch_insert_into_clickhouse_table,
                            table_name, rows)
            for table_name, rows in tasks
        ]
        for future in futures:
            future.result()
        done_cb(len(tasks), sum(len(rows) for _, rows in tasks))
        sleep(5)
