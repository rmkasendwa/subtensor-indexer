from shared.block_metadata import get_block_metadata
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

            prometheus_block UInt64 CODEC(Delta, ZSTD),
            prometheus_version UInt32 CODEC(Delta, ZSTD),
            prometheus_ip UInt128 CODEC(Delta, ZSTD),
            prometheus_port UInt16 CODEC(Delta, ZSTD),
            prometheus_ip_type UInt8 CODEC(Delta, ZSTD),

            rank UInt16 CODEC(Delta, ZSTD),
            emission UInt64 CODEC(Delta, ZSTD),
            incentive UInt16 CODEC(Delta, ZSTD),
            consensus UInt16 CODEC(Delta, ZSTD),
            trust UInt16 CODEC(Delta, ZSTD),
            validator_trust UInt16 CODEC(Delta, ZSTD),
            dividends UInt16 CODEC(Delta, ZSTD),
            last_update UInt64 CODEC(Delta, ZSTD),

            validator_permit Bool CODEC(Delta, ZSTD),
            pruning_score UInt16 CODEC(Delta, ZSTD),
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (subnet_id, neuron_id, timestamp)
        """
        get_clickhouse_client().execute(query)


def do_process_block(n):
    print(rust_bindings)
    substrate = get_substrate_client()
    # Create table if it doesn't exist

    (block_timestamp, block_hash) = get_block_metadata(3_500_000)

    start = time.time()
    rust = rust_bindings.query(block_hash)
    print("rust took ", time.time() - start)
    start = time.time()
    py = substrate.query_map(
        "SubtensorModule", "Stake",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in py:
        pass
    print("py took ", time.time() - start)
    exit(1)

    start = time.time()
    active = substrate.query_map(
        "SubtensorModule", "Active",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in active:
        pass
    rank = substrate.query_map(
        "SubtensorModule", "Rank",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in rank:
        pass
    trust = substrate.query_map(
        "SubtensorModule", "Trust",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in trust:
        pass
    emission = substrate.query_map(
        "SubtensorModule", "Emission",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in emission:
        pass
    consensus = substrate.query_map(
        "SubtensorModule", "Consensus",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in consensus:
        pass
    incentive = substrate.query_map(
        "SubtensorModule", "Incentive",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in incentive:
        pass
    dividends = substrate.query_map(
        "SubtensorModule", "Dividends",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in dividends:
        pass
    last_update = substrate.query_map(
        "SubtensorModule", "LastUpdate",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in last_update:
        pass
    pruning_scores = substrate.query_map(
        "SubtensorModule", "PruningScores",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in pruning_scores:
        pass
    validator_trust = substrate.query_map(
        "SubtensorModule", "ValidatorTrust",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in validator_trust:
        pass
    validator_permit = substrate.query_map(
        "SubtensorModule", "ValidatorPermit",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in validator_permit:
        pass
    axons = substrate.query_map(
        "SubtensorModule", "Axons",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in axons:
        pass
    prometheus = substrate.query_map(
        "SubtensorModule", "Prometheus",
        page_size=1000,
        block_hash=block_hash
    )
    for _ in prometheus:
        pass

    end = time.time()
    print(f"Took {end - start} seconds")

    # for ((hotkey, coldkey), value) in stakes:
    #     buffer_insert("shovel_stake_double_map", [n, block_timestamp, f"'{
    #                   hotkey}'", f"'{coldkey}'", value.value])

# def do_process_block(n):
#     substrate = get_substrate_client()
#     # Create table if it doesn't exist
#
#     (block_timestamp, block_hash) = get_block_metadata(3_500_000)
#
#     subnets = []
#     result = substrate.query_map(
#         'SubtensorModule', 'NetworksAdded',
#         block_hash=block_hash,
#         page_size=1000
#     )
#     for r in result:
#         print(r[0].value)
#         subnets.append(r[0].value)
#
#     start = time.time()
#     coldkeys_keys = []
#     axon_keys = []
#     prometheus_keys = []
#     start = time.time()
#
#     ac
#
#     for subnet_id in subnets:
#         active_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Active", (subnet_id,)
#         )
#         rank_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Rank", (subnet_id,)
#         )
#         trust_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Trust", (subnet_id,)
#         )
#         emission_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Emission", (subnet_id,)
#         )
#         consensus_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Consensus", (subnet_id,)
#         )
#         incentive_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Incentive", (subnet_id,)
#         )
#         dividends_vec_key = create_storage_key_cached(
#             "SubtensorModule", "Dividends", (subnet_id,)
#         )
#         last_update_vec_key = create_storage_key_cached(
#             "SubtensorModule", "LastUpdate", (subnet_id,)
#         )
#         pruning_scores_vec_key = create_storage_key_cached(
#             "SubtensorModule", "PruningScores", (subnet_id,)
#         )
#         validator_trust_vec_key = create_storage_key_cached(
#             "SubtensorModule", "ValidatorTrust", (subnet_id,)
#         )
#         validator_permit_vec_key = create_storage_key_cached(
#             "SubtensorModule", "ValidatorPermit", (subnet_id,)
#         )
#         n_neurons_key = create_storage_key_cached(
#             "SubtensorModule", "SubnetworkN", (subnet_id,)
#         )
#
#         start = time.time()
#         active_vec, rank_vec, trust_vec, emission_vec, consensus_vec, incentive_vec, dividends_vec, last_update_vec, pruning_scores_vec, validator_trust_vec, validator_permit_vec, n_neurons = substrate.query_multi(
#             [
#                 active_vec_key,
#                 rank_vec_key,
#                 trust_vec_key,
#                 emission_vec_key,
#                 consensus_vec_key,
#                 incentive_vec_key,
#                 dividends_vec_key,
#                 last_update_vec_key,
#                 pruning_scores_vec_key,
#                 validator_trust_vec_key,
#                 validator_permit_vec_key,
#                 n_neurons_key,
#             ],
#             block_hash=block_hash,
#         )
#         n_neurons = n_neurons[1].value
#         start = time.time()
#
#         hotkeys_keys = [
#             create_storage_key_cached(
#                 "SubtensorModule", "Keys", (subnet_id, neuron_id)
#             ) for neuron_id in range(n_neurons)
#         ]
#         hotkeys = substrate.query_multi(
#             hotkeys_keys, block_hash
#         )
#         hotkeys = map(lambda x: x[1].value, hotkeys)
#
#         _coldkeys_keys = [
#             create_storage_key_cached(
#                 "SubtensorModule", "Owner", (hotkey,)
#             )
#             for hotkey in hotkeys
#         ]
#
#         _axon_keys = [
#             create_storage_key_cached(
#                 "SubtensorModule", "Axons", (subnet_id, hotkey)
#             )
#             for hotkey in hotkeys
#         ]
#
#         _prometheus_keys = [
#             create_storage_key_cached(
#                 "SubtensorModule", "Prometheus", (subnet_id, hotkey))
#             for hotkey in hotkeys
#         ]
#         coldkeys_keys.extend(_coldkeys_keys)
#         prometheus_keys.extend(_prometheus_keys)
#         axon_keys.extend(_axon_keys)
#         # r = substrate.query_multi(
#         #     coldkeys_keys + axon_keys + prometheus_keys,
#         #     block_hash=block_hash,
#         # )
#
#         end = time.time()
#         print(f"Took {end - start} seconds")
#
#     r = substrate.query_multi(
#         coldkeys_keys + axon_keys + prometheus_keys,
#         block_hash=block_hash,
#     )
#     print(f"all done in {time.time() - start}")
#
#     # for ((hotkey, coldkey), value) in stakes:
#     #     buffer_insert("shovel_stake_double_map", [n, block_timestamp, f"'{
#     #                   hotkey}'", f"'{coldkey}'", value.value])


def main():
    SubnetsShovel(name="subnets").start()


if __name__ == "__main__":
    main()
