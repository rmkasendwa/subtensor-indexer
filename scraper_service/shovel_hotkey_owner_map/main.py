from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

last_proof = None

last_owners = []

OWNERS_PREFIX = "0x658faa385070e074c85bf6b568cf0555eca6b7a1fdc9f689184ecb4f359c0518"


def check_root_read_proof(block_hash):
    """Check if the owner map has changed using storage proof."""
    global last_proof

    try:
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
    except Exception as e:
        raise ShovelProcessingError(f"Failed to check root read proof: {str(e)}")


class HotkeyOwnerMapShovel(ShovelBaseClass):
    table_name = "shovel_hotkey_owner_map"

    def process_block(self, n):
        do_process_block(self, n)


def do_process_block(self, n):
    global last_owners

    try:
        try:
            substrate = get_substrate_client()
        except Exception as e:
            raise ShovelProcessingError(f"Failed to initialize substrate client: {str(e)}")

        # Create table if it doesn't exist
        try:
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
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            map_changed = check_root_read_proof(block_hash)
        except ShovelProcessingError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to check if owner map changed: {str(e)}")

        try:
            # Store owners for every block for fast queries
            owners = last_owners if map_changed is False else substrate.query_map(
                "SubtensorModule",
                "Owner",
                block_hash=block_hash,
                page_size=1000
            )

            if not owners and n != 0:
                raise ShovelProcessingError(f"No owner data returned for block {n}")

            if map_changed:
                last_owners = owners

            try:
                for (hotkey, coldkey) in owners:
                    buffer_insert(
                        self.table_name,
                        [n, block_timestamp, f"'{hotkey}'", f"'{coldkey}'"]
                    )
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

        except DatabaseConnectionError:
            raise
        except ShovelProcessingError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process owner data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def main():
    HotkeyOwnerMapShovel(name="hotkey_owner_map").start()


if __name__ == "__main__":
    main()
