from shared.substrate import reconnect_substrate
from tenacity import retry, stop_after_attempt, wait_fixed
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
import logging
import rust_bindings
from shovel_subnets.utils import create_table, get_axon_cache, get_coldkeys_and_stakes, refresh_axon_cache, default_axon
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class SubnetsShovel(ShovelBaseClass):
    def process_block(self, n):
        try:
            do_process_block(n)
        except Exception as e:
            if isinstance(e, (DatabaseConnectionError, ShovelProcessingError)):
                raise
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")


@retry(
    wait=wait_fixed(2),
    before_sleep=lambda _: reconnect_substrate(),
    stop=stop_after_attempt(15)
)
def do_process_block(n):
    try:
        # Create table if it doesn't exist
        try:
            create_table()
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/verify table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            (neurons, hotkeys) = rust_bindings.query_neuron_info(block_hash)
            if neurons is None or hotkeys is None:
                raise ShovelProcessingError("Received None response from query_neuron_info")
        except Exception as e:
            raise ShovelProcessingError(f"Failed to query neuron info: {str(e)}")

        try:
            coldkeys_and_stakes = get_coldkeys_and_stakes(
                hotkeys, block_timestamp, block_hash, n
            )
            if coldkeys_and_stakes is None:
                raise ShovelProcessingError("Received None response from get_coldkeys_and_stakes")
        except Exception as e:
            if isinstance(e, DatabaseConnectionError):
                raise
            raise ShovelProcessingError(f"Failed to get coldkeys and stakes: {str(e)}")

        try:
            refresh_axon_cache(block_timestamp, block_hash, n)
        except Exception as e:
            if isinstance(e, DatabaseConnectionError):
                raise
            raise ShovelProcessingError(f"Failed to refresh axon cache: {str(e)}")

        axon_cache = get_axon_cache()

        try:
            for neuron in neurons:
                subnet_id = neuron.subnet_id
                hotkey = neuron.hotkey
                axon = axon_cache.get((subnet_id, hotkey)) or default_axon
                coldkey_and_stake = coldkeys_and_stakes.get(hotkey)
                if coldkey_and_stake is None:
                    logging.error(f"{hotkey} has no coldkey and stake!")
                    raise ShovelProcessingError(f"Neuron {hotkey} has no coldkey and stake data")

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

                    neuron.validator_permit,
                    neuron.pruning_scores  # pruning_score UInt16 CODEC(Delta, ZSTD)
                ])
        except Exception as e:
            if isinstance(e, DatabaseConnectionError):
                raise
            raise ShovelProcessingError(f"Failed to process neuron data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Catch any other unexpected errors and wrap them
        raise ShovelProcessingError(f"Unexpected error in do_process_block: {str(e)}")


def main():
    SubnetsShovel(name="subnets").start()


if __name__ == "__main__":
    main()
