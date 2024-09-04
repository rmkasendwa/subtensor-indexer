from shared.block_metadata import get_block_metadata
from tqdm import tqdm
from datetime import datetime
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import create_storage_key_cached, get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
import logging
import time
import rust_bindings


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class SubnetsShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


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
            axon_ip UInt128 CODEC(Delta, ZSTD),
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
            weights Array((u16, u16)) CODEC(Delta, ZSTD),
            last_update UInt64 CODEC(Delta, ZSTD),

            validator_permit Bool CODEC(Delta, ZSTD),
            pruning_score UInt16 CODEC(Delta, ZSTD),
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (subnet_id, neuron_id, timestamp)
        """
        get_clickhouse_client().execute(query)


cache = {}


def batch(iterable, n=1):
    length = len(iterable)
    for i in range(0, length, n):
        yield iterable[i:i + n]


def get_coldkeys_and_stakes(hotkeys, timestamp, block_hash):
    global cache

    need_to_query = []
    for hotkey in hotkeys:
        if (hotkey, timestamp) not in cache:
            need_to_query.append(hotkey)

    if len(need_to_query) > 0:
        clickhouse = get_clickhouse_client()
        batch_size = 1000
        responses = []
        dt_object = datetime.fromtimestamp(timestamp)
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
            cache[(int(response[0].timestamp()), response[1])] = (
                response[2], response[3]
            )

    coldkeys_and_stakes = dict()
    for hotkey in hotkeys:
        # handle a hotkey without a stake
        if (timestamp, hotkey) not in cache:
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
            cache[(timestamp, hotkey)] = (coldkey, stake)

        # we should have stakes for all hotkeys now!
        coldkey, stake = cache[(timestamp, hotkey)]
        coldkeys_and_stakes[hotkey] = (coldkey, stake)

    return coldkeys_and_stakes


def do_process_block(n):
    print(rust_bindings)
    substrate = get_substrate_client()
    # Create table if it doesn't exist

    (block_timestamp, block_hash) = get_block_metadata(1_400_000)

    start = time.time()
    (neurons, hotkeys) = rust_bindings.query_neuron_info(block_hash)
    print("rust took ", time.time() - start)

    coldkeys_and_stakes = get_coldkeys_and_stakes(
        hotkeys, block_timestamp, block_hash
    )

    exit(1)

    # todo: get axon info


def main():
    SubnetsShovel(name="subnets").start()


if __name__ == "__main__":
    main()
