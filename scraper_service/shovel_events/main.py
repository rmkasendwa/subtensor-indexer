from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.clickhouse.utils import (
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
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


def do_process_block(n):
    try:
        try:
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to initialize block processing: {str(e)}")

        try:
            events = substrate.query(
                "System",
                "Events",
                block_hash=block_hash,
            )
            if not events and n != 0:
                raise ShovelProcessingError(f"No events returned for block {n}")
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch events from substrate: {str(e)}")

        # Needed to handle edge case of duplicate events in the same block
        event_id = 0
        for e in events:
            try:
                event = e.value["event"]
                # Let column generation errors propagate up - we want to fail on new event types
                (column_names, column_types, values) = generate_column_definitions(
                    event["attributes"]
                )

                try:
                    table_name = get_table_name(
                        event["module_id"], event["event_id"], tuple(column_names)
                    )

                    # Dynamically create table if not exists
                    if not table_exists(table_name):
                        create_clickhouse_table(
                            table_name, column_names, column_types, values)

                except Exception as e:
                    raise DatabaseConnectionError(f"Failed to create/check table {table_name}: {str(e)}")

                try:
                    # Insert event data into table
                    all_values = [
                        n,
                        block_timestamp,
                        event_id,
                    ] + values
                    buffer_insert(table_name, all_values)
                    event_id += 1
                except Exception as e:
                    raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

            except DatabaseConnectionError:
                raise
            except Exception as e:
                # Convert any other errors to ShovelProcessingError to fail the shovel
                raise ShovelProcessingError(f"Failed to process event in block {n}: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


if __name__ == "__main__":
    main()
