from shared.clickhouse.batch_insert import buffer_insert, flush_buffer, batch_insert_into_clickhouse_table
from shared.substrate import get_substrate_client
from time import sleep
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
    skip_interval = 1

    def __init__(self, name, skip_interval=1):
        """
        Choose a unique name for the shovel.
        """
        self.name = name
        self.skip_interval = skip_interval
        self.starting_block = 0  # Default value, can be overridden by subclasses

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
        logging.info(f"Last scraped block is {last_scraped_block_number}")

        # Create a list of block numbers to scrape
        while True:
            block_numbers = tqdm(
                range(last_scraped_block_number +
                      1, finalized_block_number + 1, self.skip_interval)
            )

            if len(block_numbers) > 0:
                logging.info(
                    f"Catching up {len(block_numbers)} blocks")
                for block_number in block_numbers:
                    self.process_block(block_number)
                    self.checkpoint_block_number = block_number
            else:
                logging.info(
                    "Already up to latest finalized block, checking again in 12s...")

            # Make sure to sleep so buffer with checkpoint update is flushed to Clickhouse
            # before trying again
            sleep(12)
            last_scraped_block_number = self.get_checkpoint()
            finalized_block_hash = substrate.get_chain_finalised_head()
            finalized_block_number = substrate.get_block_number(
                finalized_block_hash)

    def process_block(self, n):
        raise NotImplementedError(
            "Please implement the process_block method in your shovel class!"
        )

    def _buffer_flush_started(self):
        self.last_buffer_flush_call_block_number = self.checkpoint_block_number

    def _buffer_flush_done(self, tables, rows):
        if self.last_buffer_flush_call_block_number == 0:
            return

        print(
            f"Block {self.last_buffer_flush_call_block_number}: Flushed {
                rows} rows across {tables} tables to Clickhouse"
        )

        # Create checkpoint table if it doesn't exist
        if not table_exists("shovel_checkpoints"):
            query = """
            CREATE TABLE IF NOT EXISTS shovel_checkpoints (
                shovel_name String,
                block_number UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (shovel_name)
            """
            get_clickhouse_client().execute(query)

        # Update checkpoint
        buffer_insert(
            "shovel_checkpoints",
            [f"'{self.name}'", self.last_buffer_flush_call_block_number],
        )

    def get_checkpoint(self):
        if not table_exists("shovel_checkpoints"):
            return -1
        query = f"""
            SELECT block_number
            FROM shovel_checkpoints
            WHERE shovel_name = '{self.name}'
            ORDER BY block_number DESC
            LIMIT 1
        """
        res = get_clickhouse_client().execute(query)
        if res:
            return res[0][0]
        else:
            return self.starting_block
