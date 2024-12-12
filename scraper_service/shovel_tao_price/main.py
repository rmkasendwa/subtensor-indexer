import logging

from cmc_client import get_price_by_time, CMC_TOKEN
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from tenacity import retry, wait_fixed

BLOCKS_A_DAY = (24 * 60 * 60) / 12
BLOCKS_IN_36_S = 36/12

# After this block change the interval from daily to every 36s
THRESHOLD_BLOCK = 4249779

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class TaoPriceShovel(ShovelBaseClass):
    table_name = "shovel_tao_price"
    starting_block=2137

    def process_block(self, n):
        # `skip_interval` has a hiccup sometimes
        # for unknown reasons and its not elastic
        # enough to handle conditions
        if n > THRESHOLD_BLOCK:
            if n % BLOCKS_IN_36_S != 0:
                return
        else:
            if n % BLOCKS_A_DAY != 0:
                return
        do_process_block(n, self.table_name)

@retry(
    wait=wait_fixed(30),
)
def do_process_block(n, table_name):
    substrate = get_substrate_client()

    if not table_exists(table_name):
        first_run(table_name)

    block_hash = substrate.get_block_hash(n)
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )

    if block_timestamp == 0:
        return

    latest_price_data = get_price_by_time(block_timestamp)

    if latest_price_data:
        buffer_insert(table_name, [block_timestamp, *latest_price_data])
    else:
        raise Exception("Rate limit error encountered. Waiting before retrying...")



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


def main():
    if not CMC_TOKEN:
        logging.error("CMC_TOKEN is not set. Doing nothing...")
    else:
        TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("An error occurred: %s", e)
        exit(1)
