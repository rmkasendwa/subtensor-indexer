from shared.block_metadata import get_block_metadata
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

last_stakes_proof = None

STAKES_PREFIX = "0x658faa385070e074c85bf6b568cf055522fbe0bd0cb77b6b6f365f641b0de381"


def check_root_read_proof(block_hash):
    global last_stakes_proof

    substrate = get_substrate_client()
    r = substrate.rpc_request(
        "state_getReadProof",
        params=[[STAKES_PREFIX], block_hash]
    )
    this_stakes_proof = set(r["result"]["proof"])
    stake_map_changed = last_stakes_proof is None or last_stakes_proof.isdisjoint(
        this_stakes_proof
    )
    last_stakes_proof = this_stakes_proof

    return stake_map_changed


class StakeDoubleMapShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


def do_process_block(n):
    # Create table if it doesn't exist
    if not table_exists("shovel_stake_double_map"):
        query = """
        CREATE TABLE IF NOT EXISTS shovel_stake_double_map (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            hotkey String CODEC(ZSTD),
            coldkey String CODEC(ZSTD),
            stake UInt64 CODEC(Delta, ZSTD)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (hotkey, coldkey, block_number, timestamp)
        """
        get_clickhouse_client().execute(query)

    (block_timestamp, block_hash) = get_block_metadata(n)
    stake_map_changed = check_root_read_proof(block_hash)

    if stake_map_changed is False:
        # No change to stake map, noop
        return

    stakes = get_substrate_client().query_map(
        "SubtensorModule",
        "Stake",
        block_hash=block_hash,
        page_size=1000
    )

    for ((hotkey, coldkey), value) in stakes:
        buffer_insert("shovel_stake_double_map", [n, block_timestamp, f"'{
                      hotkey}'", f"'{coldkey}'", value.value])


def main():
    StakeDoubleMapShovel(name="stake_double_map").start()


if __name__ == "__main__":
    main()
