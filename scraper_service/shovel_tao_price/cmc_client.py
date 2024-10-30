import requests
import logging
import os
from datetime import datetime

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

def get_historical_prices():
    parameters = {
        'id': CMC_TAO_ID,
        'convert': 'USD',
        'interval': '24h',
        'time_start': FIRST_TAO_LISTING_DAY.strftime('%Y-%m-%d') + 'T21:37:00',
        'count': min((datetime.now() - FIRST_TAO_LISTING_DAY).days, 700)
    }

    data, status_code = fetch_cmc_data(parameters, 'historical')

    if status_code == 200 and 'data' in data and 'quotes' in data['data']:
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
