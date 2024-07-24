from substrateinterface.base import is_valid_ss58_address
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert, flush_buffer
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    escape_column_name,
    get_clickhouse_client,
    table_exists,
)
from tqdm import tqdm
import logging
import threading
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


def format_value(value):
    """
    SQL requires strings to be wrapped in single quotes
    """
    if isinstance(value, str):
        return f"'{value}'"
    else:
        return value


def get_column_type(value):
    if isinstance(value, str):
        return "String"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif value is None:
        return None
    else:
        print(f"Unhandled type: {type(value)}")
        return "String"


def generate_column_definitions(item, parent_key=None):
    columns = []

    if isinstance(item, dict):
        for key, value in item.items():
            column_name = f"{parent_key}__{key}" if parent_key else key
            columns.extend(generate_column_definitions(value, column_name))

    elif isinstance(item, tuple):
        for i, item in enumerate(item):
            item_key = "tuple" + "_" + str(i)
            item_name = f"{parent_key}.{item_key}" if parent_key else item_key
            columns.extend(generate_column_definitions(item, item_name))

    else:
        column_type = get_column_type(item)
        if column_type is not None:
            column_name = parent_key if parent_key else "value"
            columns.append((column_name, column_type, format_value(item)))

    return columns


def create_clickhouse_table(table_name, values):
    additional_columns = [
        "block_number UInt64",
        "timestamp DateTime",
    ]

    columns = list(map(lambda x: f"{escape_column_name(x[0])} {x[1]}", values))
    all_columns = additional_columns + columns
    column_definitions = ", ".join(all_columns)
    # Need to escape reserved column names

    # OrderBy block_number, timestamp, then any addresses
    order_by = ["block_number", "timestamp"]
    for value in values:
        if isinstance(value[2], str) and is_valid_ss58_address(value[2]):
            order_by.append(value[0])

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_definitions}
    ) ENGINE = ReplacingMergeTree()
    ORDER BY ({", ".join(order_by)})
    """

    get_clickhouse_client().execute(sql)


class EventsShovel(ShovelBaseClass):
    def process_block(self, n):
        substrate = get_substrate_client()

        (block_timestamp, block_hash) = get_block_metadata(n)

        events = substrate.query(
            "System",
            "Events",
            block_hash=block_hash,
        )

        for e in events:
            event = e.value["event"]
            table_name = event["module_id"] + \
                "_" + event["event_id"] + "_events"
            columns = generate_column_definitions(event["attributes"])

            # Dynamically create table if not exists
            if not table_exists(table_name):
                create_clickhouse_table(table_name, columns)

            # Insert event data into table
            event_values = list(map(lambda x: x[2], columns))
            all_values = [n, block_timestamp] + event_values
            buffer_insert(table_name, all_values)

        logging.info(f"Scraped block {n} ({block_hash}): {
            len(events.value)} event/s")


def main():
    EventsShovel().start()


if __name__ == "__main__":
    main()
