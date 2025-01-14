import logging

from cmc_client import get_price_by_time, CMC_TOKEN
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.block_metadata import get_block_metadata

BLOCKS_A_DAY = (24 * 60 * 60) / 12
FETCH_EVERY_N_BLOCKS = (60 * 5) / 12

# After this block change the interval from daily to every 5 mins
THRESHOLD_BLOCK = 4249779

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class TaoPriceShovel(ShovelBaseClass):
    table_name = "shovel_tao_price"
    starting_block = 2137

    def process_block(self, n):
        try:
            # `skip_interval` has a hiccup sometimes
            # for unknown reasons and its not elastic
            # enough to handle conditions
            if n > THRESHOLD_BLOCK:
                if n % FETCH_EVERY_N_BLOCKS != 0:
                    return
            else:
                if n % BLOCKS_A_DAY != 0:
                    return
            do_process_block(n, self.table_name)
        except Exception as e:
            if isinstance(e, (DatabaseConnectionError, ShovelProcessingError)):
                raise
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")


def do_process_block(n, table_name):
    try:
        # Create table if it doesn't exist
        try:
            if not table_exists(table_name):
                first_run(table_name)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/verify table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
            if block_timestamp == 0:
                raise ShovelProcessingError(f"Invalid block timestamp 0 for block {n}")
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            latest_price_data = get_price_by_time(block_timestamp)
            if latest_price_data is None:
                raise ShovelProcessingError("Failed to fetch price data from CMC")
        except Exception as e:
            if isinstance(e, ShovelProcessingError):
                raise
            raise ShovelProcessingError(f"Failed to get price data: {str(e)}")

        try:
            buffer_insert(table_name, [block_timestamp, *latest_price_data])
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Catch any other unexpected errors and wrap them
        raise ShovelProcessingError(f"Unexpected error in do_process_block: {str(e)}")


def first_run(table_name):
    try:
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DateTime CODEC(Delta, ZSTD),
            price Float64 CODEC(ZSTD),
            market_cap Float64 CODEC(ZSTD),
            volume Float64 CODEC(ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY timestamp
        """
        get_clickhouse_client().execute(query)
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to create table: {str(e)}")


def main():
    if not CMC_TOKEN:
        logging.error("CMC_TOKEN is not set")
        return
    TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    main()
