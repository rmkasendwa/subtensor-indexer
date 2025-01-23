import requests
import logging
import os
from datetime import datetime, timedelta
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError

CMC_TAO_ID = 22974
CMC_TOKEN = os.getenv("CMC_TOKEN")
FIRST_TAO_LISTING_DAY = datetime(2023, 3, 6)

def fetch_cmc_data(params, endpoint):
    try:
        if not CMC_TOKEN:
            raise ShovelProcessingError("CMC_TOKEN is not set")

        url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/{endpoint}"
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': CMC_TOKEN
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)  # Add timeout
        except requests.Timeout:
            logging.warning("CMC API request timed out")
            raise ShovelProcessingError("CMC API request timed out")
        except requests.ConnectionError:
            logging.warning("Failed to connect to CMC API")
            raise ShovelProcessingError("Failed to connect to CMC API")

        # Handle rate limiting explicitly
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logging.warning(f"CMC API rate limit exceeded, retry after {retry_after} seconds")
            raise ShovelProcessingError("CMC API rate limit exceeded")

        # Handle other common error codes
        if response.status_code == 401:
            raise ShovelProcessingError("Invalid CMC API key")
        elif response.status_code == 403:
            raise ShovelProcessingError("CMC API access forbidden")
        elif response.status_code >= 500:
            logging.warning(f"CMC API server error: {response.status_code}")
            raise ShovelProcessingError(f"CMC API server error: {response.status_code}")
        elif response.status_code != 200:
            logging.warning(f"CMC API request failed with status code: {response.status_code}")
            raise ShovelProcessingError(f"CMC API request failed with status code: {response.status_code}")

        try:
            data = response.json()
        except ValueError:
            logging.warning("Failed to parse CMC API response as JSON")
            raise ShovelProcessingError("Failed to parse CMC API response as JSON")

        # Check for API-level errors
        if 'status' in data and 'error_code' in data['status'] and data['status']['error_code'] != 0:
            error_message = data['status'].get('error_message', 'Unknown API error')
            logging.warning(f"CMC API error: {error_message}")
            raise ShovelProcessingError(f"CMC API error: {error_message}")

        if 'data' not in data:
            logging.warning("Invalid CMC API response: missing 'data' field")
            raise DatabaseConnectionError("Invalid CMC API response: missing 'data' field")
        if 'quotes' not in data['data']:
            logging.warning("Invalid CMC API response: missing 'quotes' field")
            raise DatabaseConnectionError("Invalid CMC API response: missing 'quotes' field")
        if not data['data']['quotes']:
            logging.warning(f"No price data available for timestamp {params.get('time_start', 'latest')}")
            raise DatabaseConnectionError(f"No price data available for timestamp {params.get('time_start', 'latest')}")

        quote = data['data']['quotes'][0] if endpoint == 'historical' else data['data'][str(CMC_TAO_ID)]
        if 'quote' not in quote or 'USD' not in quote['quote']:
            logging.warning("Invalid CMC API response: missing USD quote data")
            raise DatabaseConnectionError("Invalid CMC API response: missing USD quote data")

        usd_quote = quote['quote']['USD']
        required_fields = ['price', 'market_cap', 'volume_24h']
        for field in required_fields:
            if field not in usd_quote:
                logging.warning(f"Invalid CMC API response: missing {field} field")
                raise DatabaseConnectionError(f"Invalid CMC API response: missing {field} field")
            if usd_quote[field] is None:
                logging.warning(f"Invalid CMC API response: {field} is None")
                raise DatabaseConnectionError(f"Invalid CMC API response: {field} is None")

        return data, response.status_code
    except ShovelProcessingError:
        raise
    except Exception as e:
        logging.warning(f"Unexpected error in CMC API request: {str(e)}")
        raise DatabaseConnectionError(f"Unexpected error in CMC API request: {str(e)}")

def get_price_by_time(timestamp):
    if timestamp is None or timestamp <= 0:
        raise ShovelProcessingError("Invalid timestamp provided")

    try:
        # Calculate the time 48 hours ago from now
        time_48_hours_ago = datetime.now() - timedelta(hours=48)
        logging.debug(f"48 hours ago: {time_48_hours_ago}")

        # Determine the interval based on the timestamp
        timestamp_dt = datetime.fromtimestamp(timestamp)
        logging.debug(f"Timestamp as datetime: {timestamp_dt}")

        # Validate timestamp is not before TAO listing
        if timestamp_dt < FIRST_TAO_LISTING_DAY:
            raise ShovelProcessingError(f"Timestamp {timestamp_dt} is before TAO listing date {FIRST_TAO_LISTING_DAY}")

        if timestamp_dt > time_48_hours_ago:
            interval = '5m'
            logging.debug("Using 5m interval (within last 48 hours)")
        else:
            interval = '24h'
            logging.debug("Using 24h interval (older than 48 hours)")

        parameters = {
            'id': CMC_TAO_ID,
            'convert': 'USD',
            'interval': interval,
            'time_start': timestamp,
            'count': 1
        }
        logging.debug(f"Request parameters: {parameters}")

        data, status_code = fetch_cmc_data(parameters, 'historical')
        logging.debug(f"Got response with status code: {status_code}")

        if 'data' not in data:
            raise DatabaseConnectionError(f"Invalid CMC API response: missing 'data' field")
        if 'quotes' not in data['data']:
            raise DatabaseConnectionError(f"Invalid CMC API response: missing 'quotes' field")
        if not data['data']['quotes']:
            raise DatabaseConnectionError(f"No price data available for timestamp {timestamp}")

        quote = data['data']['quotes'][0]
        if 'quote' not in quote or 'USD' not in quote['quote']:
            raise DatabaseConnectionError(f"Invalid CMC API response: missing USD quote data")

        usd_quote = quote['quote']['USD']
        required_fields = ['price', 'market_cap', 'volume_24h']
        for field in required_fields:
            if field not in usd_quote:
                raise DatabaseConnectionError(f"Invalid CMC API response: missing {field} field")
            if usd_quote[field] is None:
                raise DatabaseConnectionError(f"Invalid CMC API response: {field} is None")

        price = usd_quote['price']
        market_cap = usd_quote['market_cap']
        volume = usd_quote['volume_24h']

        # Validate values
        if price < 0 or market_cap < 0 or volume < 0:
            raise ShovelProcessingError(f"Invalid negative values in price data: price={price}, market_cap={market_cap}, volume={volume}")

        logging.debug(f"Returning price={price}, market_cap={market_cap}, volume={volume}")
        return price, market_cap, volume

    except ShovelProcessingError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to get price data: {str(e)}")

def get_latest_price():
    try:
        parameters = {
            'id': CMC_TAO_ID,
            'convert': 'USD'
        }

        data, status_code = fetch_cmc_data(parameters, 'latest')

        if 'data' not in data:
            raise DatabaseConnectionError(f"Invalid CMC API response: missing 'data' field")

        tao_id_str = str(CMC_TAO_ID)
        if tao_id_str not in data['data']:
            raise DatabaseConnectionError(f"No data available for TAO (ID: {CMC_TAO_ID})")

        tao_data = data['data'][tao_id_str]
        if 'quote' not in tao_data or 'USD' not in tao_data['quote']:
            raise DatabaseConnectionError(f"Invalid CMC API response: missing USD quote data")

        usd_quote = tao_data['quote']['USD']
        required_fields = ['price', 'market_cap', 'volume_24h']
        for field in required_fields:
            if field not in usd_quote:
                raise DatabaseConnectionError(f"Invalid CMC API response: missing {field} field")
            if usd_quote[field] is None:
                raise DatabaseConnectionError(f"Invalid CMC API response: {field} is None")

        price = usd_quote['price']
        market_cap = usd_quote['market_cap']
        volume = usd_quote['volume_24h']

        # Validate values
        if price < 0 or market_cap < 0 or volume < 0:
            raise ShovelProcessingError(f"Invalid negative values in price data: price={price}, market_cap={market_cap}, volume={volume}")

        return price, market_cap, volume

    except ShovelProcessingError:
        raise
    except Exception as e:
        raise DatabaseConnectionError(f"Failed to get latest price data: {str(e)}")
