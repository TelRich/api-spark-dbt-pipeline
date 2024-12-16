#%%
import os
import sys
import yaml
from alpha_vantage import AlphaVantageWrapper
import pandas as pd
from logging_config import configure_logger

# Configure logger
logger = configure_logger()

def search_and_save_symbol(symbol):
    try:
        # Log script initialization
        logger.info("Starting the Alpha Vantage ETL process.")

        # Load configuration from YAML file
        config_path = "config.yaml"
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            sys.exit(1)
        logger.info(f"Loading configuration from: {config_path}")
        config = yaml.safe_load(open(config_path, "r"))

        # Create Alpha Vantage wrapper
        logger.info("Initializing Alpha Vantage Wrapper.")
        alpha_v = AlphaVantageWrapper(config)

        # Check if the symbol already exists in the database
        query = """SELECT symbol FROM search_symbols WHERE symbol = %s"""
        try:
            db_symbols = pd.read_sql(query, alpha_v.mysql_engine, params=(symbol,))['symbol'].tolist()
            if symbol in db_symbols:
                logger.info(f"Symbol '{symbol}' already exists in the database. Skipping API call.")
                return
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            return

        # Search for the symbol
        logger.info(f"Searching for symbol: '{symbol}'.")
        status_code, search_results = alpha_v.search_symbol("SYMBOL_SEARCH", f"keywords={symbol}")

        # Check and log the status code and results
        if status_code == 200:
            logger.info(f"Search successful with status code: {status_code}.")
            if 'bestMatches' in search_results:
                logger.info(f"Number of search results: {len(search_results['bestMatches'])}")
            else:
                logger.warning("No 'bestMatches' found in the search results.")
                search_results['bestMatches'] = []
        else:
            logger.error(f"Search failed with status code: {status_code}. Exiting process.")
            return

        # Process search results into a DataFrame
        search_df = pd.DataFrame(search_results['bestMatches'])
        if not search_df.empty:
            logger.info("Transforming search results into DataFrame.")
            search_df.rename(columns={
                '1. symbol': 'symbol',
                '2. name': 'name',
                '3. type': 'type',
                '4. region': 'region',
                '5. marketOpen': 'marketOpen',
                '6. marketClose': 'marketClose',
                '7. timezone': 'timezone',
                '8. currency': 'currency',
                '9. matchScore': 'matchScore'
            }, inplace=True)
        else:
            logger.warning("Search results are empty. Skipping database save.")
            return

        # Save the DataFrame to MySQL
        logger.info("Saving search results to MySQL database.")
        alpha_v.save_to_mysql(search_df, 'search_symbols')
        logger.info("Data saved to MySQL database under the table 'search_symbols'.")

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    search_and_save_symbol('IBM')

