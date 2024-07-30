from tenacity import retry, stop_after_attempt, wait_fixed

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.clickhouse.utils import (
    table_exists,
)
import logging

from shovel_events.utils import (
    create_clickhouse_table,
    generate_column_definitions,
    get_table_name,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class EventsShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


def main():
    EventsShovel(name="events").start()


@retry(
    wait=wait_fixed(2),
    before_sleep=lambda _: reconnect_substrate(),
    stop=stop_after_attempt(15)
)
def do_process_block(n):
    substrate = get_substrate_client()

    (block_timestamp, block_hash) = get_block_metadata(n)

    events = substrate.query(
        "System",
        "Events",
        block_hash=block_hash,
    )

    # Needed to handle edge case of duplicate events in the same block
    event_id = 0
    for e in events:
        event = e.value["event"]
        (column_names, column_types, values) = generate_column_definitions(
            event["attributes"]
        )

        table_name = get_table_name(
            event["module_id"], event["event_id"], tuple(column_names)
        )

        # Dynamically create table if not exists
        if not table_exists(table_name):
            create_clickhouse_table(
                table_name, column_names, column_types, values)

        # Insert event data into table
        all_values = [
            n,
            block_timestamp,
            event_id,
        ] + values
        buffer_insert(table_name, all_values)
        event_id += 1


if __name__ == "__main__":
    main()
