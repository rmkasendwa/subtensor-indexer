import logging
import requests
import os
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


CMC_TAO_ID = 22974
CMC_TOKEN = os.getenv("CMC_TOKEN")

class TaoPriceShovel(ShovelBaseClass):
    table_name = "shovel_tao_price"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    substrate = get_substrate_client()

    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DateTime CODEC(Delta, ZSTD),
            price Float64 CODEC(ZSTD),
            market_cap Float64 CODEC(ZSTD)
            volume Float64 CODEC(ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY block_number
        """
        get_clickhouse_client().execute(query)

    block_hash = substrate.get_block_hash(n)
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )

    buffer_insert(self.table_name, [n, block_timestamp])

def first_run():
    'elo'

def fetch_tao_price():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical"
    parameters = {
        'id': CMC_TAO_ID,
        'convert': 'USD',
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_TOKEN
    }

    response = requests.get(url, headers=headers, params=parameters)
    data = response.json()

    if response.status_code == 200 and 'data' in data and 'TAO' in data['data']:
        tao_data = data['data']['TAO']
        price = tao_data['quote']['USD']['price']
        market_cap = tao_data['quote']['USD']['market_cap']
        volume = tao_data['quote']['USD']['volume_24h']
        return price, market_cap, volume
    else:
        logging.error("Failed to fetch TAO price: %s", data.get('status', {}).get('error_message', 'Unknown error'))
        return None, None, None

def main():
    TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    main()
