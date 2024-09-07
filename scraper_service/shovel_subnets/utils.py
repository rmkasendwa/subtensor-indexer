from datetime import datetime
from tqdm import tqdm
import time
import rust_bindings
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from collections import namedtuple

Axon = namedtuple('Axon', ['block', 'version', 'ip', 'port',
                  'ip_type', 'protocol', 'placeholder1', 'placeholder2'])
default_axon = Axon(block=0, version=0, ip=0, port=0,
                    ip_type=0, protocol=0, placeholder1=0, placeholder2=0)


def create_table():
    if not table_exists("shovel_subnets"):
        query = """
        CREATE TABLE IF NOT EXISTS shovel_subnets (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            subnet_id UInt16 CODEC(Delta, ZSTD),
            neuron_id UInt16 CODEC(Delta, ZSTD),

            hotkey String CODEC(ZSTD),
            coldkey String CODEC(ZSTD),
            active Bool CODEC(ZSTD),

            axon_block UInt64 CODEC(Delta, ZSTD),
            axon_version UInt32 CODEC(Delta, ZSTD),
            axon_ip String CODEC(ZSTD),
            axon_port UInt16 CODEC(Delta, ZSTD),
            axon_ip_type UInt8 CODEC(Delta, ZSTD),
            axon_protocol UInt8 CODEC(Delta, ZSTD),
            axon_placeholder1 UInt8 CODEC(Delta, ZSTD),
            axon_placeholder2 UInt8 CODEC(Delta, ZSTD),

            rank UInt16 CODEC(Delta, ZSTD),
            emission UInt64 CODEC(Delta, ZSTD),
            incentive UInt16 CODEC(Delta, ZSTD),
            consensus UInt16 CODEC(Delta, ZSTD),
            trust UInt16 CODEC(Delta, ZSTD),
            validator_trust UInt16 CODEC(Delta, ZSTD),
            dividends UInt16 CODEC(Delta, ZSTD),
            stake UInt64 CODEC(Delta, ZSTD),
            weights Array(Tuple(UInt16, UInt16)) CODEC(ZSTD),
            bonds Array(Tuple(UInt16, UInt16)) CODEC(ZSTD),
            last_update UInt64 CODEC(Delta, ZSTD),

            validator_permit Bool CODEC(Delta, ZSTD),
            pruning_scores UInt16 CODEC(Delta, ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (subnet_id, neuron_id, timestamp)
        """
        get_clickhouse_client().execute(query)


axon_extrinsics_cache = {}


axon_cache = {}


def get_axon_cache():
    return axon_cache


def refresh_axon_cache(block_timestamp, block_hash, block_number):
    global axon_cache

    extrinsics_synced_block_query = "SELECT block_number FROM test_db.shovel_checkpoints FINAL WHERE shovel_name = 'extrinsics';"
    extrinsics_synced_block = get_clickhouse_client().execute(
        extrinsics_synced_block_query)[0][0]

    while (extrinsics_synced_block < block_number):
        print("Waiting for extrinsics table to sync...")
        time.sleep(60)
        extrinsics_synced_block = get_clickhouse_client().execute(
            extrinsics_synced_block_query)[0][0]

    # Init the axon cache on first run
    if len(axon_cache) == 0:
        print("Initializing axon cache...")
        axon_cache = rust_bindings.query_axons(block_hash)
        print("Initialized axon cache")

    # Get events this block
    if block_timestamp not in axon_extrinsics_cache:
        # get any axon extrinsics this block
        dt_object = datetime.fromtimestamp(block_timestamp)
        formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
            SELECT address, arg_netuid, arg_version, arg_ip, arg_port, arg_ip_type, arg_protocol, arg_placeholder1, arg_placeholder2
            FROM shovel_extrinsics_SubtensorModule_serve_axon_v0
            WHERE timestamp = '{formatted_date}' AND success = True
        """
        axon_events = get_clickhouse_client().execute(query)
        axon_extrinsics_cache[block_timestamp] = axon_events

        for e in axon_events:
            hotkey = e[0]
            subnet_id = e[1]
            version = e[2]
            ip = e[3]
            port = e[4]
            ip_type = e[5]
            protocol = e[6]
            placeholder1 = e[7]
            placeholder2 = e[8]

            print(f"Updating axon cache for {subnet_id}:{hotkey}")
            axon_cache[(subnet_id, hotkey)] = Axon(
                block=block_number,
                version=version,
                ip=ip,
                port=port,
                ip_type=ip_type,
                protocol=protocol,
                placeholder1=placeholder1,
                placeholder2=placeholder2
            )


coldkey_stake_cache = {}


def batch(iterable, n=1):
    length = len(iterable)
    for i in range(0, length, n):
        yield iterable[i:i + n]


def get_coldkeys_and_stakes(hotkeys, block_timestamp, block_hash, block_number):
    global coldkey_stake_cache

    stake_map_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'stake_double_map';"
    hotkey_owner_map_synced_block = get_clickhouse_client().execute(
        stake_map_synced_block_query)[0][0]
    hotkey_owner_map_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'hotkey_owner_map';"
    hotkey_owner_map_synced_block = get_clickhouse_client().execute(
        hotkey_owner_map_synced_block_query)[0][0]

    while (hotkey_owner_map_synced_block < block_number or hotkey_owner_map_synced_block < block_number):
        print("Waiting for stake_double_map and hotkey_owner_map tables to sync...")
        time.sleep(60)
        hotkey_owner_map_synced_block = get_clickhouse_client().execute(
            stake_map_synced_block_query)[0][0]
        hotkey_owner_map_synced_block = get_clickhouse_client().execute(
            hotkey_owner_map_synced_block_query)[0][0]

    need_to_query = []
    for hotkey in hotkeys:
        if (block_timestamp, hotkey) not in coldkey_stake_cache:
            need_to_query.append(hotkey)

    if len(need_to_query) > 0:
        clickhouse = get_clickhouse_client()
        batch_size = 1000
        responses = []
        dt_object = datetime.fromtimestamp(block_timestamp)
        formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Quering {len(need_to_query) /
                       batch_size} batches of coldkeys and stakes"
        )
        for hotkey_batch in tqdm(batch(need_to_query, batch_size)):
            hotkeys_list = "', '".join(hotkey_batch)
            hotkeys_str = f"('{hotkeys_list}')"
            query = f"""
                    SELECT
                        timestamp,
                        hotkey,
                        coldkey,
                        stake
                    FROM test_db.shovel_hotkey_owner_map AS o
                    INNER JOIN test_db.shovel_stake_double_map AS s
                    ON o.timestamp = s.timestamp AND o.coldkey = s.coldkey AND o.hotkey = s.hotkey
                    WHERE hotkey IN {hotkeys_str}
                    AND timestamp >= '{formatted_date}' AND timestamp < addHours('{formatted_date}', 1)
                """
            r = clickhouse.execute(query)
            responses.extend(r)
        for response in responses:
            coldkey_stake_cache[(int(response[0].timestamp()), response[1])] = (
                response[2], response[3]
            )

    coldkeys_and_stakes = dict()
    for hotkey in hotkeys:
        # handle a hotkey without a stake
        if (block_timestamp, hotkey) not in coldkey_stake_cache:
            coldkey = get_substrate_client().query(
                "SubtensorModule",
                "Owner",
                block_hash=block_hash,
                params=[hotkey],
            ).value
            stake = get_substrate_client().query(
                "SubtensorModule",
                "Stake",
                block_hash=block_hash,
                params=[hotkey, coldkey],
            )
            if stake != 0:
                print(
                    f"ERROR: Hotkey {hotkey} has stake {
                        stake} but not in Clickhouse"
                )
                exit(0)
            coldkey_stake_cache[(block_timestamp, hotkey)] = (coldkey, stake)

        # we should have stakes for all hotkeys now!
        coldkey, stake = coldkey_stake_cache[(block_timestamp, hotkey)]
        coldkeys_and_stakes[hotkey] = (coldkey, stake)

    # periodically clear out historical keys
    if len(coldkey_stake_cache) > 10_000_000:
        print("Clearing historical coldkey_stake_cache")
        deleted = 0
        prev_length = len(coldkey_stake_cache)
        keys_to_delete = []
        for (timestamp, hotkey) in coldkey_stake_cache.keys():
            if timestamp < block_timestamp:
                keys_to_delete.append((timestamp, hotkey))
        for key in keys_to_delete:
            del coldkey_stake_cache[key]
            deleted += 1
        print(f"Deleted {deleted} keys, {
              prev_length - len(coldkey_stake_cache)} remain")

    return coldkeys_and_stakes
