import logging
import requests
import os
from datetime import datetime
# from shared.clickhouse.batch_insert import buffer_insert
# from shared.shovel_base_class import ShovelBaseClass
# from shared.substrate import get_substrate_client
# from shared.clickhouse.utils import (
#     get_clickhouse_client,
#     table_exists,
# )


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


CMC_TAO_ID = 22974
CMC_TOKEN = os.getenv("CMC_TOKEN")
FIRST_TAO_LISTING_DAY = datetime(2023, 3, 6)

# class TaoPriceShovel(ShovelBaseClass):
#     table_name = "shovel_tao_price"

#     def process_block(self, n):
#         do_process_block(n, self.table_name)


# def do_process_block(n, table_name):
#     substrate = get_substrate_client()

#     if not table_exists(table_name):
#         query = f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             timestamp DateTime CODEC(Delta, ZSTD),
#             price Float64 CODEC(ZSTD),
#             market_cap Float64 CODEC(ZSTD)
#             volume Float64 CODEC(ZSTD)
#         ) ENGINE = ReplacingMergeTree()
#         PARTITION BY toYYYYMM(timestamp)
#         ORDER BY block_number
#         """
#         get_clickhouse_client().execute(query)

#     block_hash = substrate.get_block_hash(n)
#     block_timestamp = int(
#         substrate.query(
#             "Timestamp",
#             "Now",
#             block_hash=block_hash,
#         ).serialize()
#         / 1000
#     )

#     buffer_insert(table_name, [n, block_timestamp])


def first_run():
    'elo'

def fetch_historical_prices():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical"
    parameters = {
        'id': CMC_TAO_ID,
        'convert': 'USD',
        'interval': 'daily',
        'time_start': FIRST_TAO_LISTING_DAY.strftime('%Y-%m-%d'),
        'count': min((datetime.now() - FIRST_TAO_LISTING_DAY).days, 700)
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_TOKEN
    }

    response = requests.get(url, headers=headers, params=parameters)
    data = response.json()

    if response.status_code == 200 and 'data' in data and 'quotes' in data['data']:
        quotes = data['data']['quotes']
        results = []
        for quote in quotes:
            timestamp = quote['timestamp']
            usd_quote = quote['quote']['USD']
            price = usd_quote['price']
            market_cap = usd_quote['market_cap']
            volume = usd_quote['volume_24h']
            results.append((timestamp, price, market_cap, volume))
        return results
    else:
        logging.error("Failed to fetch TAO price: %s", data.get('status', {}).get('error_message', 'Unknown error'))
        return []

def main():
    res = fetch_historical_prices()
    print(res)
    # TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    main()
