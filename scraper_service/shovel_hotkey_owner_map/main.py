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

last_proof = None

last_owners = []

OWNERS_PREFIX = "0x658faa385070e074c85bf6b568cf0555eca6b7a1fdc9f689184ecb4f359c0518"


def check_root_read_proof(block_hash):
    global last_proof

    substrate = get_substrate_client()
    r = substrate.rpc_request(
        "state_getReadProof",
        params=[[OWNERS_PREFIX], block_hash]
    )
    this_stakes_proof = set(r["result"]["proof"])
    map_changed = last_proof is None or last_proof.isdisjoint(
        this_stakes_proof
    )
    last_proof = this_stakes_proof

    return map_changed


class HotkeyOwnerMapShovel(ShovelBaseClass):
    table_name = "shovel_hotkey_owner_map"

    def process_block(self, n):
        do_process_block(self, n)


def do_process_block(self, n):
    global last_owners
    substrate = get_substrate_client()

    # Create table if it doesn't exist
    if not table_exists(self.table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            hotkey String CODEC(ZSTD),
            coldkey String CODEC(ZSTD),
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (hotkey, coldkey, timestamp)
        """
        get_clickhouse_client().execute(query)

    (block_timestamp, block_hash) = get_block_metadata(n)
    map_changed = check_root_read_proof(block_hash)

    # Store owners for every block for fast queries
    owners = last_owners if map_changed is False else substrate.query_map(
        "SubtensorModule",
        "Owner",
        block_hash=block_hash,
        page_size=1000
    )

    if map_changed:
        last_owners = owners

    for (hotkey, coldkey) in owners:
        buffer_insert(
            self.table_name,
            [n, block_timestamp, f"'{hotkey}'", f"'{coldkey}'"]
        )


def main():
    HotkeyOwnerMapShovel(name="hotkey_owner_map").start()


if __name__ == "__main__":
    main()
