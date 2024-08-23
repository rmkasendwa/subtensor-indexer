from functools import lru_cache
import time
from datetime import datetime
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
import logging
from collections import defaultdict


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
    table_name = "shovel_stake_double_map"

    def process_block(self, n):
        do_process_block(n, self.table_name)


prev_pending_emissions = {}

stake_map = dict()


def do_process_block(n, table_name):
    substrate = get_substrate_client()
    # Create table if it doesn't exist
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            hotkey String CODEC(ZSTD),
            coldkey String CODEC(ZSTD),
            stake UInt64 CODEC(Delta, ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (coldkey, hotkey, timestamp)
        """
        get_clickhouse_client().execute(query)

    if not table_exists("agg_stake_events"):
        query = """
        CREATE VIEW agg_stake_events
        (
            `block_number` UInt64,
            `timestamp` DateTime,
            `hotkey` String,
            `coldkey` String,
            `amount` Int64,
            `operation` String
        )
        AS SELECT
            l.block_number AS block_number,
            l.timestamp AS timestamp,
            l.tuple_0 AS hotkey,
            r.coldkey AS coldkey,
            l.tuple_1 AS amount,
            'remove' AS operation
        FROM test_db.shovel_hotkey_owner_map AS r
        INNER JOIN test_db.shovel_events_SubtensorModule_StakeRemoved_v0 AS l ON (l.tuple_0 = r.hotkey) AND (l.timestamp = r.timestamp)
        UNION ALL
        SELECT
            sa.block_number,
            sa.timestamp,
            sa.tuple_0 AS hotkey,
            r.coldkey,
            sa.tuple_1 AS amount,
            'add' AS operation
        FROM test_db.shovel_hotkey_owner_map AS r
        INNER JOIN test_db.shovel_events_SubtensorModule_StakeAdded_v0 AS sa ON (sa.tuple_0 = r.hotkey) AND (sa.timestamp = r.timestamp);
        """
        get_clickhouse_client().execute(query)

    (block_timestamp, block_hash) = get_block_metadata(n)

    hotkeys_needing_update = set()

    # Get pending emission amount for every subnet
    result = substrate.query_map(
        'SubtensorModule', 'PendingEmission',
        block_hash=block_hash,
        page_size=1000
    )
    for subnet_id_scale, pending_emission_scale in result:
        pending_emission = pending_emission_scale.value
        subnet_id = subnet_id_scale.value
        if (subnet_id not in prev_pending_emissions) or pending_emission == 0 and prev_pending_emissions[subnet_id] != 0:
            print(
                f"Refreshing all hotkeys from subnet {
                    subnet_id} due to update..."
            )
            subnet_hotkeys = substrate.query_map(
                "SubtensorModule",
                "Keys",
                params=[subnet_id],
                block_hash=block_hash,
                page_size=1000
            )
            count = 0
            for (key, hotkey) in subnet_hotkeys:
                hotkeys_needing_update.add(hotkey.value)
                count += 1
            print(
                f"Found {count} hotkeys for {subnet_id}"
            )

        prev_pending_emissions[subnet_id] = pending_emission

    # Check if we're up to date
    events_synced_block_query = "SELECT block_number FROM test_db.shovel_checkpoints FINAL WHERE shovel_name = 'events';"
    events_synced_block = get_clickhouse_client().execute(
        events_synced_block_query)[0][0]
    hotkey_owner_map_synced_block_query = "SELECT block_number FROM test_db.shovel_checkpoints FINAL WHERE shovel_name = 'hotkey_owner_map';"
    hotkey_owner_map_synced_block = get_clickhouse_client().execute(
        hotkey_owner_map_synced_block_query)[0][0]

    while (events_synced_block < n or hotkey_owner_map_synced_block < n):
        print("Waiting for events and hotkey_owner_map tables to sync...")
        time.sleep(60)
        events_synced_block = get_clickhouse_client().execute(
            events_synced_block_query)[0][0]
        hotkey_owner_map_synced_block = get_clickhouse_client().execute(
            hotkey_owner_map_synced_block_query)[0][0]

    # Get hotkeys with stake events this block
    dt_object = datetime.fromtimestamp(block_timestamp)
    formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
    distinct_hotkeys_query = f"""
        SELECT DISTINCT(hotkey) from test_db.agg_stake_events WHERE timestamp = '{formatted_date}'
    """
    distinct_hotkeys = get_clickhouse_client().execute(distinct_hotkeys_query)

    for r in distinct_hotkeys:
        hotkeys_needing_update.add(r[0])

    # Get agg stake events for this block
    for (i, hotkey) in enumerate(hotkeys_needing_update):
        r = get_substrate_client().query_map(
            "SubtensorModule",
            "Stake",
            block_hash=block_hash,
            page_size=1000,
            params=[hotkey],
        )
        for (coldkey_scale, stake) in r:
            coldkey = coldkey_scale.value
            stake_map[(hotkey, coldkey)] = stake

    for ((hotkey, coldkey), stake) in stake_map.items():
        buffer_insert(
            table_name,
            [n, block_timestamp, f"'{hotkey}'", f"'{coldkey}'", stake]
        )


@lru_cache
def create_storage_key_cached(pallet, storage, args):
    return get_substrate_client().create_storage_key(pallet, storage, list(args))


def main():
    StakeDoubleMapShovel(name="stake_double_map").start()


if __name__ == "__main__":
    main()
