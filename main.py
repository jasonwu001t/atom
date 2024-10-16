from fastapi import FastAPI, HTTPException, Query, Response, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import threading
import json
import math
import logging
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError
import functools
from cachetools import TTLCache, cached
from cachetools.keys import hashkey
from TAI.source import Alpaca, OptionBet
import json
import os
from pathlib import Path

app = FastAPI()

#uvicorn main_uat:app --host 127.0.0.1 --port 8001

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# CORS settings
origins = [
    "http://localhost:3000",  # Added for development
    "https://nativequant.com",
    "https://www.nativequant.com",
    "http://nativequant.com",
    "http://www.nativequant.com",
]

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Use the updated origins list
    allow_credentials=True,  # Set to False if you set allow_origins to ["*"]
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=['*'],  # Expose all headers
    max_age=600,
)

def ttl_cache(ttl=3600, maxsize=1000):
    def decorator(func):
        cache = TTLCache(maxsize=maxsize, ttl=ttl)
        lock = threading.Lock()
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = hashkey(*args, **kwargs)
            with lock:
                if key in cache:
                    return cache[key]
            result = func(*args, **kwargs)
            with lock:
                cache[key] = result
            return result
        return wrapper
    return decorator

def sanitize_data(obj):
    if isinstance(obj, dict):
        return {k: sanitize_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_data(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None  # Replace NaN or Infinity with None
        else:
            return obj
    else:
        return obj
    
# Existing FastAPI endpoints with 1-hour cache

@ttl_cache(ttl=3600)
@app.get("/describe_perc_change/{ticker}/{expiry_date}")
def describe_perc_change(
    ticker: str,
    expiry_date: str,
    lookback_period: int = Query(365 * 5)
):
    try:
        # Initialize OptionBet class
        option_bet = OptionBet(
            ticker=ticker.upper(),
            expiry_date=expiry_date,
            lookback_period=lookback_period
        )
        # Generate the JSON output using the to_json method from OptionBet class
        describe_json = option_bet.to_json()
        describe_data = json.loads(describe_json)
        return JSONResponse(
            content=describe_data,
            status_code=200
        )
    except Exception as e:
        logger.error(f"Error in describe_perc_change: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@ttl_cache(ttl=3600)
@app.get("/option_chain/{underlying_symbol}/{expiration_date}")
def option_chain(underlying_symbol: str, expiration_date: str):
    try:
        ap = Alpaca()
        json_data = ap.get_option_chain_json(
            underlying_symbol.upper(), expiration_date
        )
        return JSONResponse(
            content=json_data,
            status_code=200
        )
    except Exception as e:
        logger.error(f"Error in option_chain: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@ttl_cache(ttl=3600)
@app.get("/latest_stock_trade/{symbol_or_symbols}")
def latest_stock_trade(symbol_or_symbols: str):
    try:
        ap = Alpaca()
        json_data1 = ap.get_latest_trade(
            symbol_or_symbols.upper()
        )
        json_data = sanitize_data(json_data1)
        return JSONResponse(
            content=json_data,
            status_code=200
        )
    except Exception as e:
        logger.error(f"Error in latest_stock_trade: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@ttl_cache(ttl=3600)
@app.get("/stock_historical/{symbol}")
def stock_historical(
    symbol: str,
    lookback: int = Query(None),
    timeframe: str = Query('Day'),
    from_date: str = Query(None),
    to_date: str = Query(None)
):
    logger.info(f"Received request for stock_historical with symbol: {symbol}")
    try:
        ap = Alpaca()
        logger.info("Alpaca instance created")
        logger.info(f"Parameters: lookback={lookback}, timeframe={timeframe}, from_date={from_date}, to_date={to_date}")

        # Validate and parse dates
        from_date_dt = None
        to_date_dt = None

        if from_date:
            from_date_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if to_date:
            to_date_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Determine lookback_period based on from_date and to_date
        if from_date_dt and to_date_dt:
            delta = to_date_dt - from_date_dt
            lookback = delta.days + 1  # Include the end date
        elif from_date_dt:
            delta = ap.now - from_date_dt
            lookback = delta.days + 1
        elif not lookback:
            # Default lookback if none provided
            lookback = 30

        # Fetch historical data
        logger.info(f"Calling get_stock_historical for {symbol}")
        historical_data = ap.get_stock_historical(
            symbol,
            lookback_period=lookback,
            timeframe=timeframe,
            raw=True
        )

        logger.info(f"get_stock_historical returned: {type(historical_data)}")

        if historical_data is None or symbol.upper() not in historical_data:
            logger.error(f"No historical data available for {symbol}")
            raise HTTPException(status_code=404, detail="No historical data available")

        logger.info(f"Successfully fetched historical data for {symbol}")

        # Extract data for the symbol
        data = historical_data.get(symbol.upper(), [])

        # Filter data based on from_date and to_date
        if from_date_dt or to_date_dt:
            filtered_data = []
            for item in data:
                timestamp = datetime.fromisoformat(item['timestamp'].replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
                if from_date_dt and timestamp.date() < from_date_dt.date():
                    continue
                if to_date_dt and timestamp.date() > to_date_dt.date():
                    continue
                filtered_data.append(item)
            data = filtered_data

        # Sort data by timestamp in descending order
        data.sort(key=lambda x: x['timestamp'], reverse=True)

        # Return data in the required format
        return JSONResponse(
            content={symbol.upper(): data},
            status_code=200
        )
    except Exception as e:
        logger.error(f"Error in stock_historical: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# AWS Chalice endpoints converted to FastAPI with 24-hour cache

# Define base path for local data
BASE_PATH = Path("/home/jwu/fastapi_jtrade1/data_downloader")

# Update ECONOMY keys to use local file paths
ECONOMY_KEYS = {
    "commercial_banks_deposits": BASE_PATH / "fred/data/commercial_banks_deposits.json",
    "consumer_price_index": BASE_PATH / "fred/data/consumer_price_index.json",
    "core_cpi": BASE_PATH / "fred/data/core_cpi.json",
    "federal_funds_rate": BASE_PATH / "fred/data/federal_funds_rate.json",
    "fed_total_assets": BASE_PATH / "fred/data/fed_total_assets.json",
    "gdp": BASE_PATH / "fred/data/gdp.json",
    "m2": BASE_PATH / "fred/data/m2.json",
    "sp500": BASE_PATH / "fred/data/sp500.json",
    "total_money_market_fund": BASE_PATH / "fred/data/total_money_market_fund.json",
    "unemployment_rate": BASE_PATH / "fred/data/unemployment_rate.json",
    "us_30yr_fix_mortgage_rate": BASE_PATH / "fred/data/us_30yr_fix_mortgage_rate.json",
    "us_producer_price_index": BASE_PATH / "fred/data/us_producer_price_index.json",

    "bls_data": BASE_PATH / "bls/bls_data/bls_data.json",
    "nonfarm_payroll": BASE_PATH / "bls/bls_data/nonfarm_payroll.json",
    "unemployment_rate": BASE_PATH / "bls/bls_data/unemployment_rate.json",
    "us_avg_weekly_hours": BASE_PATH / "bls/bls_data/us_avg_weekly_hours.json",
    "us_job_opening": BASE_PATH / "bls/bls_data/us_job_opening.json",
}

ECONOMY_KEYS_SHORT = {
    "commercial_banks_deposits": BASE_PATH / "fred/data/short_commercial_banks_deposits.json",
    "consumer_price_index": BASE_PATH / "fred/data/short_consumer_price_index.json",
    "core_cpi": BASE_PATH / "fred/data/short_core_cpi.json",
    "federal_funds_rate": BASE_PATH / "fred/data/short_federal_funds_rate.json",
    "fed_total_assets": BASE_PATH / "fred/data/short_fed_total_assets.json",
    "gdp": BASE_PATH / "fred/data/short_gdp.json",
    "m2": BASE_PATH / "fred/data/short_m2.json",
    "sp500": BASE_PATH / "fred/data/short_sp500.json",
    "total_money_market_fund": BASE_PATH / "fred/data/short_total_money_market_fund.json",
    "unemployment_rate": BASE_PATH / "fred/data/short_unemployment_rate.json",
    "us_30yr_fix_mortgage_rate": BASE_PATH / "fred/data/short_us_30yr_fix_mortgage_rate.json",
    "us_producer_price_index": BASE_PATH / "fred/data/short_us_producer_price_index.json",
    "bls_data": BASE_PATH / "bls/bls_data/bls_data_short.json",
    "nonfarm_payroll": BASE_PATH / "bls/bls_data/nonfarm_payroll_short.json",
    "unemployment_rate": BASE_PATH / "bls/bls_data/unemployment_rate_short.json",
    "us_avg_weekly_hours": BASE_PATH / "bls/bls_data/us_avg_weekly_hours_short.json",
    "us_job_opening": BASE_PATH / "bls/bls_data/us_job_opening_short.json",               
}

# Update other categories to use local file paths
GENERIC_KEYS = {
    "us_treasury_yield": BASE_PATH / "us_treasury_curve/data/treasury_yield_all.json",
    "articles": BASE_PATH / "static_data/articles.json",
    "categories": BASE_PATH / "static_data/categories.json",
    # "chart": BASE_PATH / "chart_data.json",
    # "indicators": BASE_PATH / "static_data/indicators.json",
}

def fetch_json_from_local(file_path: Path):
    """Fetch JSON data from local file system and sanitize it."""
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        # Sanitize data
        sanitized_data = sanitize_data(data)
        return sanitized_data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return {"error": f"File not found: {file_path}"}
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON in {file_path}")
        return {"error": f"Error decoding JSON in {file_path}"}
    
s3_client = boto3.client('s3')
BUCKET_NAME = 'jtrade1-dir'  # S3 bucket name for the app
SUBSCRIPTION_KEY = 'subscriptions.json'  # Key for storing subscription data

def upload_json_to_s3(key: str, data: dict):
    """Upload JSON data to S3 bucket."""
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data))
    except ClientError as e:
        logger.error(f"Unable to upload {key} to S3: {str(e)}")
        return {"error": f"Unable to upload {key} to S3: {str(e)}"}
    
def combine_data_from_local(file_paths: list):
    combined_data = []
    for file_path in file_paths:
        data = fetch_json_from_local(file_path)
        if "error" not in data:
            # Assuming the data is a list of dictionaries
            combined_data.extend(data)
    return combined_data

# ECONOMY specific endpoints with 24-hour cache

@ttl_cache(ttl=86400)
@app.get('/economy/{indicator}', response_class=JSONResponse)
def get_economy_data(indicator: str):
    file_path = ECONOMY_KEYS.get(indicator)
    if file_path:
        data = fetch_json_from_local(file_path)
        if "error" in data:
            raise HTTPException(status_code=500, detail=data["error"])
        return data
    else:
        raise HTTPException(status_code=404, detail=f'Indicator {indicator} not found')

@ttl_cache(ttl=86400)
@app.get('/economy', response_class=JSONResponse)
def get_combined_economy_data():
    combined_economy_data = combine_data_from_local(list(ECONOMY_KEYS.values()))
    return combined_economy_data

def filter_recent_data(data, months=12):
    """
    Filter data to include only the most recent entries (default last 12 months).
    
    :param data: List of dictionaries containing economy data
    :param months: Number of months to include (default 12)
    :return: Filtered list of dictionaries
    """
    current_date = datetime.now()
    cutoff_date = current_date - timedelta(days=30 * months)
    
    filtered_data = []
    for item in data:
        if 'date' in item:
            try:
                item_date = datetime.strptime(item['date'], '%Y-%m-%d')
                if item_date >= cutoff_date:
                    filtered_data.append(item)
            except ValueError:
                # If date parsing fails, include the item anyway
                filtered_data.append(item)
        else:
            # If there's no date field, include the item
            filtered_data.append(item)
    
    return filtered_data

@ttl_cache(ttl=86400)
@app.get('/economy_short', response_class=JSONResponse)
def get_short_economy_data(mode: str = Query('yearly', enum=['yearly', 'recent'])):
    combined_economy_data = combine_data_from_local(list(ECONOMY_KEYS_SHORT.values()))
    if mode == 'yearly':
        filtered_data = combined_economy_data  # Data is already short
    elif mode == 'recent':
        filtered_data = filter_recent_data(combined_economy_data)
    else:
        raise HTTPException(status_code=400, detail='Invalid filter mode. Use "yearly" or "recent".')
    return filtered_data

@ttl_cache(ttl=86400)
@app.get('/economy_short/{indicator}', response_class=JSONResponse)
def get_short_economy_indicator_data(indicator: str, mode: str = Query('yearly', enum=['yearly', 'recent'])):
    file_path = ECONOMY_KEYS_SHORT.get(indicator)
    if not file_path:
        raise HTTPException(status_code=404, detail=f'Indicator {indicator} not found')
    data = fetch_json_from_local(file_path)
    if "error" in data:
        raise HTTPException(status_code=500, detail=data["error"])
    if mode == 'yearly':
        filtered_data = data  # Data is already short
    elif mode == 'recent':
        filtered_data = filter_recent_data([data])[0]
    else:
        raise HTTPException(status_code=400, detail='Invalid filter mode. Use "yearly" or "recent".')
    return filtered_data

# Generic category-based endpoint (Non-FRED, Non-BLS) with 24-hour cache

@ttl_cache(ttl=86400)
@app.get('/{category}/{key}', response_class=JSONResponse)
def get_generic_data_with_key(category: str, key: str):
    if category == 'articles':
        articles = fetch_json_from_local(GENERIC_KEYS['articles'])
        if "error" in articles:
            raise HTTPException(status_code=500, detail=articles["error"])
        # Filter articles by ID
        article = next((a for a in articles if a['id'] == key), None)
        if article:
            return article
        else:
            raise HTTPException(status_code=404, detail=f'Article with id {key} not found')
    else:
        # For other categories with a key, return error
        raise HTTPException(status_code=400, detail='Invalid request')

@ttl_cache(ttl=86400)
@app.get('/{category}', response_class=JSONResponse)
def get_generic_data(category: str):
    if category in ['articles', 'categories', 'us_treasury_yield']:
        file_path = GENERIC_KEYS.get(category)
        if file_path:
            with open(file_path, "r") as file:
                data = json.load(file)
                sanitized_data = sanitize_data(data)
            return sanitized_data
    else:
        file_path = GENERIC_KEYS.get(category)
        if file_path:
            data = fetch_json_from_local(file_path)
            if "error" in data:
                raise HTTPException(status_code=500, detail=data["error"])
            return data
        else:
            raise HTTPException(status_code=404, detail=f'Category {category} not found')

@ttl_cache(ttl=86400)
@app.get('/stocks/daily_ohlc/{symbol}', response_class=JSONResponse)
def get_daily_ohlc(symbol: str, from_date: str = Query(None), to_date: str = Query(None)):
    key = f'api/stock_daily_bar/{symbol.upper()}.json'
    data = fetch_json_from_local(GENERIC_KEYS[key])
    if "error" in data:
        raise HTTPException(status_code=404, detail=f'Stock data for {symbol.upper()} not found. Details: {data["error"]}')

    # Initialize date filters
    start_date = None
    end_date = None

    # Parse 'from' and 'to' query parameters
    date_format = "%Y-%m-%d"

    if from_date:
        try:
            start_date = datetime.strptime(from_date, date_format).date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid 'from' date format. Expected YYYY-MM-DD.")

    if to_date:
        try:
            end_date = datetime.strptime(to_date, date_format).date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid 'to' date format. Expected YYYY-MM-DD.")

    # Filter data by date if filters are provided
    if start_date or end_date:
        filtered_data = []
        for record in data:
            # Parse the date in each record
            try:
                record_date = datetime.strptime(record['date'], "%Y-%m-%d").date()
            except ValueError:
                continue  # Skip records with invalid date format

            # Apply the date filters
            if start_date and record_date < start_date:
                continue
            if end_date and record_date > end_date:
                continue
            filtered_data.append(record)
        data = filtered_data

    return data

# POST subscription endpoint without caching

@app.post('/subscribe', response_class=JSONResponse)
def subscribe_user(subscription: dict):
    # Validate the input data
    name = subscription.get('name')
    email = subscription.get('email')
    # Default to False if not provided
    subscribe_to_email_list = subscription.get('subscribe', False)

    if not name or not email:
        raise HTTPException(status_code=400, detail='Name and email are required.')

    # Fetch existing subscription data from S3
    subscriptions = fetch_json_from_local(GENERIC_KEYS['subscriptions.json'])
    if "error" in subscriptions:
        raise HTTPException(status_code=500, detail=subscriptions['error'])

    # Add the new subscription
    new_subscription = {
        'name': name,
        'email': email,
        'subscribe': subscribe_to_email_list
    }

    if isinstance(subscriptions, list):
        subscriptions.append(new_subscription)
    else:
        subscriptions = [new_subscription]

    # Upload updated subscription list to S3
    upload_error = upload_json_to_s3(GENERIC_KEYS['subscriptions.json'], subscriptions)
    if upload_error:
        raise HTTPException(status_code=500, detail=upload_error['error'])

    return {'message': 'Subscription successful.'}