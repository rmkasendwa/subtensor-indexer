from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class BlockTimestampShovel(ShovelBaseClass):
    table_name = "shovel_block_timestamp"

    def process_block(self, n):
        do_process_block(self, n)


def do_process_block(self, n):
    substrate = get_substrate_client()

    # Create table if it doesn't exist
    if not table_exists(self.table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY block_number
        """
        get_clickhouse_client().execute(query)

    block_hash = substrate.get_block_hash(n)
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )

    buffer_insert(self.table_name, [n, block_timestamp])


def main():
    BlockTimestampShovel(name="block_timestamps").start()


if __name__ == "__main__":
    main()
