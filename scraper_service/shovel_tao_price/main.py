import logging
from cmc_client import get_historical_prices, get_latest_price
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class TaoPriceShovel(ShovelBaseClass):
    table_name = "shovel_tao_price"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    substrate = get_substrate_client()

    if not table_exists(table_name):
        first_run()

    block_hash = substrate.get_block_hash(n)
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )
    latest_price_data = get_latest_price()

    if latest_price_data:
        buffer_insert(table_name, [block_timestamp, *latest_price_data])



def first_run(table_name):
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

    historical_prices = get_historical_prices()
    for record in historical_prices:
        buffer_insert(table_name, record)


def main():
    TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    main()
