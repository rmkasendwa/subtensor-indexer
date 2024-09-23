from shared.substrate import reconnect_substrate
from tenacity import retry, stop_after_attempt, wait_fixed
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
import logging
import rust_bindings
import time
from shovel_subnets.utils import create_table, get_axon_cache, get_coldkeys_and_stakes, refresh_axon_cache, default_axon


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

# temporary hack around memory leak: restart shovel every 10mins
# TODO: find and fix the actual issue causing the leak!
start_time = time.time()


class SubnetsShovel(ShovelBaseClass):
    def process_block(self, n):
        cur_time = time.time()
        if cur_time - start_time > 600:
            logging.info("Restarting shovel to avoid memory leak.")
            exit(0)
        do_process_block(n)


@retry(
    wait=wait_fixed(2),
    before_sleep=lambda _: reconnect_substrate(),
    stop=stop_after_attempt(15)
)
def do_process_block(n):
    # Create table if it doesn't exist
    create_table()

    (block_timestamp, block_hash) = get_block_metadata(n)

    (neurons, hotkeys) = rust_bindings.query_neuron_info(block_hash)

    coldkeys_and_stakes = get_coldkeys_and_stakes(
        hotkeys, block_timestamp, block_hash, n
    )

    refresh_axon_cache(block_timestamp, block_hash, n)

    axon_cache = get_axon_cache()
    for neuron in neurons:
        subnet_id = neuron.subnet_id
        hotkey = neuron.hotkey
        axon = axon_cache.get((subnet_id, hotkey)) or default_axon
        coldkey_and_stake = coldkeys_and_stakes.get(hotkey)
        if coldkey_and_stake is None:
            print(f"{hotkey} has no coldkey and stake!")
            exit(1)

        buffer_insert("shovel_subnets", [
            n,  # block_number UInt64 CODEC(Delta, ZSTD),
            block_timestamp,  # timestamp DateTime CODEC(Delta, ZSTD),
            neuron.subnet_id,  # subnet_id UInt16 CODEC(Delta, ZSTD),
            neuron.neuron_id,  # neuron_id UInt16 CODEC(Delta, ZSTD),

            f"'{neuron.hotkey}'",  # hotkey String CODEC(ZSTD),
            f"'{coldkey_and_stake[0]}'",  # coldkey String CODEC(ZSTD),
            neuron.active,  # active Bool CODEC(ZSTD),

            axon.block,  # axon_block UInt64 CODEC(Delta, ZSTD),
            axon.version,  # axon_version UInt32 CODEC(Delta, ZSTD),
            f'{axon.ip}',  # axon_ip String CODEC(ZSTD),
            axon.port,  # axon_port UInt16 CODEC(Delta, ZSTD),
            axon.ip_type,  # axon_ip_type UInt8 CODEC(Delta, ZSTD),
            axon.protocol,  # axon_protocol UInt8 CODEC(Delta, ZSTD),
            axon.placeholder1,            # axon_placeholder1 UInt8
            axon.placeholder2,            # axon_placeholder2 UInt8

            neuron.rank,  # rank UInt16 CODEC(Delta, ZSTD),
            neuron.emission,  # emission UInt64 CODEC(Delta, ZSTD),
            neuron.incentive,  # incentive UInt16 CODEC(Delta, ZSTD),
            neuron.consensus,  # consensus UInt16 CODEC(Delta, ZSTD),
            neuron.trust,  # trust UInt16 CODEC(Delta, ZSTD),
            neuron.validator_trust,            # validator_trust UInt16
            neuron.dividends,  # dividends UInt16 CODEC(Delta, ZSTD),
            coldkey_and_stake[1],  # stake UInt64 CODEC(Delta, ZSTD),
            neuron.weights,             # weights Array(Tuple(UInt16, UInt16)),
            neuron.bonds,  # bonds Array(Tuple(UInt16, UInt16)) CODEC(ZSTD),
            neuron.last_update,  # last_update UInt64 CODEC(Delta, ZSTD),

            neuron.validator_permit,  # validator_permit Bool
            neuron.pruning_scores  # pruning_score UInt16 CODEC(Delta, ZSTD)
        ])


def main():
    SubnetsShovel(name="subnets").start()


if __name__ == "__main__":
    main()
