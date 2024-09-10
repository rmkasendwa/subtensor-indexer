from collections import defaultdict
import logging
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.substrate import get_substrate_client
from shared.shovel_base_class import ShovelBaseClass
from shared.clickhouse.batch_insert import buffer_insert
from shared.block_metadata import get_block_metadata
from datetime import datetime
import time
import rust_bindings
from tqdm import tqdm
from functools import lru_cache

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


def main():
    # StakeDoubleMapShovel(name="stake_daily_map").start()
    (block_timestamp, block_hash) = get_block_metadata(123123)
    results = rust_bindings.query_block_stakes(block_hash)
    for result in results:
        hotkey = result[0]
        coldkey, amount = result[1][0]
        print(f"BLOCK: {block_hash}, HOTKEY: {hotkey}, COLDKEY: {coldkey}, AMOUNT: {amount}")


if __name__ == "__main__":
    main()
