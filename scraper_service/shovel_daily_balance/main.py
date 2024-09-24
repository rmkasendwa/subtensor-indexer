import logging
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.clickhouse.batch_insert import buffer_insert
from shared.block_metadata import get_block_metadata
from shared.substrate import get_substrate_client

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

class BalanceDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_balance_daily_map"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            address String CODEC(ZSTD),
            balance UInt64 CODEC(Delta, ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (address, timestamp)
        """
        get_clickhouse_client().execute(query)

    (block_timestamp, block_hash) = get_block_metadata(n)

    results = fetch_all_free_balances_at_block(block_hash)
    print(f"Processing block {n}. Found {len(results)} balance entries")
    for address, balance in results.items():
        buffer_insert(
            table_name,
            [n, block_timestamp, f"'{address}'", balance]
        )


def fetch_all_free_balances_at_block(block_hash):
    substrate = get_substrate_client()
    balances = substrate.query_map(
        module='System',
        storage_function='Account',
        block_hash=block_hash,
        page_size=1000
    )

    free_balances = {}
    for address in balances:
        address_id = address[0].value
        address_info = address[1]
        free_balances[address_id] = address_info['data']['free']

    return free_balances

def main():
    BalanceDailyMapShovel(name="balance_daily_map", skip_interval=7200).start()


if __name__ == "__main__":
    main()
