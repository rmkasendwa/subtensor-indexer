from shared.clickhouse.batch_insert import buffer_insert, flush_buffer
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from tqdm import tqdm
import logging
import threading
from concurrent.futures import ThreadPoolExecutor


class ShovelBaseClass:
    checkpoint_block_number = 0
    last_buffer_flush_call_block_number = 0
    name = None

    def __init__(self, name):
        """
        Choose a unique name for the shovel.
        """
        self.name = name

    def start(self):
        print("Initialising Substrate client")
        substrate = get_substrate_client()

        print("Fetching the finalized block")
        finalized_block_hash = substrate.get_chain_finalised_head()
        finalized_block_number = substrate.get_block_number(
            finalized_block_hash)

        # Start the clickhouse buffer
        print("Starting Clickhouse buffer")
        executor = ThreadPoolExecutor(max_workers=1)
        threading.Thread(
            target=flush_buffer,
            args=(executor, self._buffer_flush_started, self._buffer_flush_done),
        ).start()

        last_scraped_block_number = self.get_checkpoint()
        logging.info(f"Starting from block {last_scraped_block_number + 1}")

        # Create a list of block numbers to scrape
        block_numbers = tqdm(
            range(last_scraped_block_number + 1, finalized_block_number + 1)
        )

        for block_number in block_numbers:
            self.process_block(block_number)
            self.checkpoint_block_number = block_number

    def process_block(self, n):
        raise NotImplementedError(
            "Please implement the process_block method in your shovel class!"
        )

    def _buffer_flush_started(self):
        self.last_buffer_flush_call_block_number = self.checkpoint_block_number

    def _buffer_flush_done(self, tables, rows):
        print(
            f"Block {self.last_buffer_flush_call_block_number}: Flushed {
                rows} rows across {tables} tables to Clickhouse"
        )

        # Create checkpoint table if it doesn't exist
        if not table_exists("shovel_checkpoint"):
            query = """
            CREATE TABLE IF NOT EXISTS shovel_checkpoint (
                shovel_name String,
                block_number UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (shovel_name)
            """
            get_clickhouse_client().execute(query)

        # Update checkpoint
        buffer_insert(
            "shovel_checkpoint",
            [f"'{self.name}'", self.last_buffer_flush_call_block_number],
        )

    def get_checkpoint(self):
        if not table_exists("shovel_checkpoint"):
            return 0
        query = f"""
            SELECT block_number
            FROM shovel_checkpoint
            WHERE shovel_name = '{self.name}'
            ORDER BY block_number DESC
            LIMIT 1
        """
        res = get_clickhouse_client().execute(query)
        if res:
            return res[0][0]
        else:
            return 0
