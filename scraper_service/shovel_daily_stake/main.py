import logging

import rust_bindings
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    try:
        # Create table if it doesn't exist
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    coldkey String CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    stake UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            results = rust_bindings.query_block_stakes(block_hash)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch stakes from substrate: {str(e)}")

        if not results:
            raise ShovelProcessingError(f"No stake data returned for block {n}")

        logging.info(f"Processing block {n}. Found {len(results)} stake entries")

        try:
            for result in results:
                coldkey, stake = result[1][0]  # First element is hotkey, second is list of (coldkey, stake) pairs
                hotkey = result[0]
                buffer_insert(
                    table_name,
                    [n, block_timestamp, f"'{coldkey}'", f"'{hotkey}'", stake]
                )
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def main():
    StakeDailyMapShovel(name="stake_daily_map", skip_interval=7200).start()


if __name__ == "__main__":
    main()
