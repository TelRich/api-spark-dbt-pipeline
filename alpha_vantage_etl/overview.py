#%%
import os
import sys
import yaml
from alpha_vantage import AlphaVantageWrapper
from logging_config import configure_logger
import pandas as pd

# Create logger 
logger = configure_logger()

def main(symbol):
    # Load configuration from YAML file
    try:
        config = yaml.safe_load(open("config.yaml", "r"))
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Create Alpha Vantage wrapper
    try:
        alpha_v = AlphaVantageWrapper(config)
    except Exception as e:
        logger.error(f"Failed to initialize AlphaVantageWrapper: {e}")
        sys.exit(1)

    # Query database for existing symbols
    query = """SELECT symbol FROM overview WHERE symbol = %s"""
    try:
        db_symbols = pd.read_sql(query, alpha_v.mysql_engine, params=(symbol,))['symbol'].tolist()
        if symbol in db_symbols:
            logger.info(f"Symbol '{symbol}' already exists in the database. Skipping API call.")
            return
    except Exception as e:
        logger.error(f"Error querying database: {e}")
        return

    function_name = "OVERVIEW"
    
    # Make request to Alpha Vantage
    try:
        status_code, overview = alpha_v.make_base3_request(function_name=function_name, parameter=f"symbol={symbol}")
        if status_code != 200:
            logger.error(f"Failed to fetch data for {symbol} with status code {status_code}")
            return
    except Exception as e:
        logger.error(f"Error making request to Alpha Vantage: {e}")
        return

    symbol_overview = pd.DataFrame([overview])
    logger.info(f"Fetched data for {symbol}: {overview}")

    # Save new data
    try:
        alpha_v.save_to_mysql(symbol_overview, 'overview')
        logger.info("Data saved to MySQL successfully.")
    except Exception as e:
        logger.error(f"Failed to save data to MySQL: {e}")

if __name__ == "__main__":
    main('IBM')