from substrateinterface.base import is_valid_ss58_address
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert, flush_buffer
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    escape_column_name,
    get_clickhouse_client,
    table_exists,
)
from tqdm import tqdm
import logging
import threading
from concurrent.futures import ThreadPoolExecutor


class ShovelBaseClass:
    def start(self):
        print("Initialising Substrate client")
        substrate = get_substrate_client()

        print("Fetching the finalized block")
        finalized_block_hash = substrate.get_chain_finalised_head()
        finalized_block_number = substrate.get_block_number(
            finalized_block_hash)

        # Start the clickhouse buffer
        print("Starting Clickhouse buffer")
        executor = ThreadPoolExecutor(max_workers=10)
        threading.Thread(target=flush_buffer, args=(executor,)).start()

        # TODO init from checkpoint
        last_scraped_block_number = 17000

        # Create a list of block numbers to scrape
        block_numbers = tqdm(
            range(last_scraped_block_number + 1, finalized_block_number + 1)
        )

        for block_number in block_numbers:
            self.process_block(block_number)

    def process_block(self, n):
        raise NotImplementedError(
            "Please implement the process_block method in your shovel class!"
        )

    def checkpoint(self, block_number):
        pass
