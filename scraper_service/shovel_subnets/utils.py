from datetime import datetime
from tqdm import tqdm
import time
import os
import rust_bindings
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from collections import namedtuple
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging

Axon = namedtuple('Axon', ['block', 'version', 'ip', 'port',
                  'ip_type', 'protocol', 'placeholder1', 'placeholder2'])
default_axon = Axon(block=0, version=0, ip=0, port=0,
                    ip_type=0, protocol=0, placeholder1=0, placeholder2=0)


def create_table():
    try:
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
            try:
                get_clickhouse_client().execute(query)
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to execute table creation query: {str(e)}")
    except Exception as e:
        if isinstance(e, DatabaseConnectionError):
            raise
        raise DatabaseConnectionError(f"Failed to create table: {str(e)}")


axon_extrinsics_cache = {}


axon_cache = {}


def get_axon_cache():
    return axon_cache


def refresh_axon_cache(block_timestamp, block_hash, block_number):
    global axon_cache

    try:
        extrinsics_synced_block_query = f"SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'extrinsics';"
        try:
            result = get_clickhouse_client().execute(extrinsics_synced_block_query)
            if not result:
                raise DatabaseConnectionError("No checkpoint found for extrinsics shovel")
            extrinsics_synced_block = result[0][0]
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to query extrinsics sync status: {str(e)}")

        while (extrinsics_synced_block < block_number):
            logging.info("Waiting for extrinsics table to sync...")
            time.sleep(60)
            try:
                result = get_clickhouse_client().execute(extrinsics_synced_block_query)
                if not result:
                    raise DatabaseConnectionError("No checkpoint found for extrinsics shovel")
                extrinsics_synced_block = result[0][0]
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to query extrinsics sync status during wait: {str(e)}")
    except Exception as e:
        if isinstance(e, DatabaseConnectionError):
            raise
        raise DatabaseConnectionError(f"Failed to check extrinsics sync status: {str(e)}")

    # Init the axon cache on first run
    if len(axon_cache) == 0:
        try:
            logging.info("Initializing axon cache...")
            axon_cache = rust_bindings.query_axons(block_hash)
            if axon_cache is None:
                raise ShovelProcessingError("Received None response from query_axons")
            logging.info("Initialized axon cache")
        except Exception as e:
            if "DatabaseConnectionError" in str(type(e)):
                raise
            raise ShovelProcessingError(f"Failed to initialize axon cache: {str(e)}")

    # Get events this block
    if block_timestamp not in axon_extrinsics_cache:
        try:
            # get any axon extrinsics this block
            dt_object = datetime.fromtimestamp(block_timestamp)
            formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            query = f"""
                SELECT address, arg_netuid, arg_version, arg_ip, arg_port, arg_ip_type, arg_protocol, arg_placeholder1, arg_placeholder2
                FROM shovel_extrinsics_SubtensorModule_serve_axon_v0
                WHERE timestamp = '{formatted_date}' AND success = True
            """
            try:
                axon_events = get_clickhouse_client().execute(query)
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to execute axon events query: {str(e)}")

            if axon_events is None:
                raise ShovelProcessingError("Received None response from axon events query")
            axon_extrinsics_cache[block_timestamp] = axon_events
        except Exception as e:
            if isinstance(e, (DatabaseConnectionError, ShovelProcessingError)):
                raise
            raise DatabaseConnectionError(f"Failed to fetch axon events: {str(e)}")

        for e in axon_events:
            try:
                if len(e) < 9:
                    raise ShovelProcessingError(f"Invalid axon event data: expected 9 fields, got {len(e)}")

                hotkey = e[0]
                subnet_id = e[1]
                version = e[2]
                ip = e[3]
                port = e[4]
                ip_type = e[5]
                protocol = e[6]
                placeholder1 = e[7]
                placeholder2 = e[8]

                if not all(x is not None for x in [hotkey, subnet_id, version, ip, port, ip_type, protocol, placeholder1, placeholder2]):
                    raise ShovelProcessingError(f"Invalid axon event data: contains None values")

                logging.info(f"Updating axon cache for {subnet_id}:{hotkey}")
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
            except Exception as e:
                if isinstance(e, ShovelProcessingError):
                    raise
                raise ShovelProcessingError(f"Failed to process axon event: {str(e)}")


coldkey_stake_cache = {}

hotkey_owner_map_synced_block = -1

stake_map_synced_block = -1

stake_map_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'stake_double_map';"
hotkey_owner_map_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'hotkey_owner_map';"


def batch(iterable, n=1):
    if not iterable:
        return []
    length = len(iterable)
    for i in range(0, length, n):
        yield iterable[i:i + n]


def get_coldkeys_and_stakes(hotkeys, block_timestamp, block_hash, block_number):
    if not hotkeys:
        raise ShovelProcessingError("Empty hotkeys list provided")

    global coldkey_stake_cache
    global stake_map_synced_block
    global hotkey_owner_map_synced_block

    try:
        while (hotkey_owner_map_synced_block < block_number or stake_map_synced_block < block_number):
            if hotkey_owner_map_synced_block > -1 and stake_map_synced_block > -1:
                logging.info("Waiting for stake_double_map and hotkey_owner_map tables to sync...")
                time.sleep(1)
            try:
                stake_result = get_clickhouse_client().execute(stake_map_synced_block_query)
                hotkey_result = get_clickhouse_client().execute(hotkey_owner_map_synced_block_query)

                if not stake_result or not hotkey_result:
                    raise DatabaseConnectionError("No checkpoint found for stake_map or hotkey_owner_map")

                stake_map_synced_block = stake_result[0][0]
                hotkey_owner_map_synced_block = hotkey_result[0][0]
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to query dependency sync status: {str(e)}")
    except Exception as e:
        if isinstance(e, DatabaseConnectionError):
            raise
        raise DatabaseConnectionError(f"Failed to check dependency sync status: {str(e)}")

    need_to_query = []
    for hotkey in hotkeys:
        if (block_timestamp, hotkey) not in coldkey_stake_cache:
            need_to_query.append(hotkey)

    # track the block timestamps because we need them to fill in zero stake
    # amount cache later
    block_timestamps = set()
    block_timestamps.add(block_timestamp)
    if len(need_to_query) > 0:
        try:
            clickhouse = get_clickhouse_client()
            batch_size = 1000
            responses = []
            dt_object = datetime.fromtimestamp(block_timestamp)
            formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            logging.info(
                f"Querying {len(need_to_query) / batch_size} batches of coldkeys and stakes"
            )
            for hotkey_batch in tqdm(batch(need_to_query, batch_size)):
                if not hotkey_batch:
                    continue

                hotkeys_list = "', '".join(hotkey_batch)
                hotkeys_str = f"('{hotkeys_list}')"
                query = f"""
                        SELECT
                            timestamp,
                            hotkey,
                            coldkey,
                            stake
                        FROM shovel_hotkey_owner_map AS o
                        INNER JOIN shovel_stake_double_map AS s
                        ON o.timestamp = s.timestamp AND o.coldkey = s.coldkey AND o.hotkey = s.hotkey
                        WHERE hotkey IN {hotkeys_str}
                        AND timestamp >= '{formatted_date}' AND timestamp < addMinutes('{formatted_date}', 30)
                    """
                try:
                    r = clickhouse.execute(query)
                    if r is None:
                        raise DatabaseConnectionError("Received None response from coldkeys and stakes query")
                    responses.extend(r)
                except Exception as e:
                    raise DatabaseConnectionError(f"Failed to execute coldkeys and stakes query: {str(e)}")
        except Exception as e:
            if isinstance(e, DatabaseConnectionError):
                raise
            raise DatabaseConnectionError(f"Failed to query coldkeys and stakes: {str(e)}")

        for response in responses:
            try:
                if len(response) < 4:
                    raise ShovelProcessingError(f"Invalid response data: expected 4 fields, got {len(response)}")

                timestamp = int(response[0].timestamp())
                hotkey = response[1]
                coldkey = response[2]
                stake = response[3]

                if not all(x is not None for x in [timestamp, hotkey, coldkey, stake]):
                    raise ShovelProcessingError(f"Invalid response data: contains None values")

                block_timestamps.add(timestamp)
                coldkey_stake_cache[(timestamp, hotkey)] = (coldkey, stake)
            except Exception as e:
                raise ShovelProcessingError(f"Failed to process response data: {str(e)}")

    coldkeys_and_stakes = dict()
    for hotkey in hotkeys:
        try:
            # handle a hotkey without a stake
            if (block_timestamp, hotkey) not in coldkey_stake_cache:
                try:
                    coldkey_result = get_substrate_client().query(
                        "SubtensorModule",
                        "Owner",
                        block_hash=block_hash,
                        params=[hotkey],
                    )
                    if coldkey_result is None or coldkey_result.value is None:
                        raise ShovelProcessingError(f"Failed to get coldkey for hotkey {hotkey}")
                    coldkey = coldkey_result.value

                    stake = get_substrate_client().query(
                        "SubtensorModule",
                        "Stake",
                        block_hash=block_hash,
                        params=[hotkey, coldkey],
                    )
                    if stake is None:
                        raise ShovelProcessingError(f"Failed to get stake for hotkey {hotkey}")
                except Exception as e:
                    raise ShovelProcessingError(f"Failed to query substrate for hotkey {hotkey}: {str(e)}")

                if stake != 0:
                    logging.error(
                        f"ERROR: Hotkey {hotkey} has stake {stake} but not in Clickhouse"
                    )
                    raise ShovelProcessingError(f"Inconsistent stake data for hotkey {hotkey}")
                for t in block_timestamps:
                    coldkey_stake_cache[(t, hotkey)] = (coldkey, stake)

            # we should have stakes for all hotkeys now!
            try:
                coldkey, stake = coldkey_stake_cache[(block_timestamp, hotkey)]
                if coldkey is None or stake is None:
                    raise ShovelProcessingError(f"Invalid cache data for hotkey {hotkey}")
                coldkeys_and_stakes[hotkey] = (coldkey, stake)
            except KeyError:
                raise ShovelProcessingError(f"Missing cache data for hotkey {hotkey}")
        except Exception as e:
            if isinstance(e, (DatabaseConnectionError, ShovelProcessingError)):
                raise
            raise ShovelProcessingError(f"Failed to process hotkey {hotkey}: {str(e)}")

    # periodically clear out historical keys
    if len(coldkey_stake_cache) > 1_000_000:
        logging.info("Clearing historical coldkey_stake_cache")
        deleted = 0
        keys_to_delete = []
        for (timestamp, hotkey) in coldkey_stake_cache.keys():
            if timestamp < block_timestamp:
                keys_to_delete.append((timestamp, hotkey))
        for key in keys_to_delete:
            del coldkey_stake_cache[key]
            deleted += 1
        logging.info(f"Deleted {deleted} keys, {len(coldkey_stake_cache)} remain")

    return coldkeys_and_stakes
