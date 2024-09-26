import logging

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from tenacity import retry, stop_after_attempt, wait_fixed

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

class BalanceDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_balance_daily_map"

    def process_block(self, n):
        do_process_block(n, self.table_name)


@retry(
    wait=wait_fixed(2),
    before_sleep=lambda _: reconnect_substrate(),
    stop=stop_after_attempt(15)
)
def do_process_block(n, table_name):
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            address String CODEC(ZSTD),
            free_balance UInt64 CODEC(Delta, ZSTD),
            reserved_balance UInt64 CODEC(Delta, ZSTD),
            frozen_balance UInt64 CODEC(Delta, ZSTD)
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
            [n, block_timestamp, f"'{address}'", balance["free"], balance["reserved"], balance["frozen"]]
        )


def fetch_all_free_balances_at_block(block_hash):
    substrate = get_substrate_client()
    raw_balances = substrate.query_map(
        module='System',
        storage_function='Account',
        block_hash=block_hash,
        page_size=1000
    )

    balances = {}
    for address in raw_balances:
        address_id = address[0].value
        address_info = address[1]
        balances[address_id] = {
            'free': address_info['data']['free'],
            'reserved': address_info['data']['reserved'],
            'frozen': address_info['data']['frozen'] if 'frozen' in address_info['data'] else int(address_info['data']['misc_frozen'].value) + int(address_info['data']['fee_frozen'].value),
        }

    return balances

def main():
    BalanceDailyMapShovel(name="balance_daily_map", skip_interval=7200).start()


if __name__ == "__main__":
    main()
