#%%
import os
import sys
import yaml
from alpha_vantage import AlphaVantageWrapper
from logging_config import configure_logger
import pandas as pd

# Create logger 
logger = configure_logger()

def main():
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

    function_name1 = "OVERVIEW"
    symbol1 = "AA"

    # Make request to Alpha Vantage
    try:
        status_code, overview = alpha_v.make_base3_request(function_name=function_name1, symbol=symbol1)
        if status_code != 200:
            logger.error(f"Failed to fetch data for {symbol1} with status code {status_code}")
            return
    except Exception as e:
        logger.error(f"Error making request to Alpha Vantage: {e}")
        return

    symbol_overview = pd.DataFrame([overview])
    logger.info(f"Fetched data for {symbol1}: {overview}")

    # Query database for existing symbols
    query = """SELECT symbol FROM overview"""
    try:
        db_symbols = pd.read_sql(query, alpha_v.mysql_engine)['symbol'].tolist()
        logger.info(f"Retrieved symbols from database: {db_symbols}")
    except Exception as e:
        logger.error(f"Error querying database: {e}")
        return

    # Filter and save new data
    if not db_symbols:
        logger.info("No symbols found in the database. Skipping data filtering.")
        filtered_results = symbol_overview
    else:
        filtered_results = symbol_overview[~symbol_overview['Symbol'].isin(db_symbols)]
        logger.info(f"Filtered results: {filtered_results}")

    try:
        alpha_v.save_to_mysql(filtered_results, 'overview')
        logger.info("Data saved to MySQL successfully.")
    except Exception as e:
        logger.error(f"Failed to save data to MySQL: {e}")

if __name__ == "__main__":
    main()