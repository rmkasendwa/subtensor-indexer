import requests
import logging
import os
from datetime import datetime, timedelta

CMC_TAO_ID = 22974
CMC_TOKEN = os.getenv("CMC_TOKEN")
FIRST_TAO_LISTING_DAY = datetime(2023, 3, 6)

def fetch_cmc_data(params, endpoint):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/{endpoint}"
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_TOKEN
    }

    response = requests.get(url, headers=headers, params=params)
    return response.json(), response.status_code

def get_price_by_time(timestamp):
    logging.info(f"Getting price for timestamp {timestamp}")

    # Calculate the time 48 hours ago from now
    time_48_hours_ago = datetime.now() - timedelta(hours=48)
    logging.info(f"48 hours ago: {time_48_hours_ago}")

    # Determine the interval based on the timestamp
    timestamp_dt = datetime.fromtimestamp(timestamp)
    logging.info(f"Timestamp as datetime: {timestamp_dt}")

    if timestamp_dt > time_48_hours_ago:
        interval = '5m'
        logging.info("Using 5m interval (within last 48 hours)")
    else:
        interval = '24h'
        logging.info("Using 24h interval (older than 48 hours)")

    parameters = {
        'id': CMC_TAO_ID,
        'convert': 'USD',
        'interval': interval,
        'time_start': timestamp,
        'count': 1
    }
    logging.info(f"Request parameters: {parameters}")

    try:
        logging.info("Fetching data from CMC API...")
        data, status_code = fetch_cmc_data(parameters, 'historical')
        logging.info(f"Got response with status code: {status_code}")
    except Exception as e:
        logging.error("Error fetching CMC data: %s", str(e))
        logging.error("Full exception:", exc_info=True)
        return None

    if status_code == 200 and 'data' in data and 'quotes' in data['data']:
        logging.info("Successfully parsed response data")
        if not data['data']['quotes']:
            logging.info("No quotes data available, skipping")
            return None
        quote = data['data']['quotes'][0]
        usd_quote = quote['quote']['USD']
        price = usd_quote['price']
        market_cap = usd_quote['market_cap']
        volume = usd_quote['volume_24h']
        logging.info(f"Returning price={price}, market_cap={market_cap}, volume={volume}")
        return price, market_cap, volume
    else:
        logging.error("Failed to fetch TAO price with parameters %s: %s", parameters, data.get('status', {}).get('error_message', 'Unknown error'))
        logging.error(f"Full response data: {data}")
        return None

def get_latest_price():
    parameters = {
        'id': CMC_TAO_ID,
        'convert': 'USD'
    }

    data, status_code = fetch_cmc_data(parameters, 'latest')

    if status_code == 200 and 'data' in data and str(CMC_TAO_ID) in data['data']:
        tao_data = data['data'][str(CMC_TAO_ID)]
        usd_quote = tao_data['quote']['USD']
        price = usd_quote['price']
        market_cap = usd_quote['market_cap']
        volume = usd_quote['volume_24h']
        return price, market_cap, volume
    else:
        logging.error("Failed to fetch latest TAO price: %s", data.get('status', {}).get('error_message', 'Unknown error'))
        return None, None, None
