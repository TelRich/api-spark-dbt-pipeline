from config import alpha_v, logger, config
import pandas as pd
from sqlalchemy import create_engine

logger.info('Starting script')

# Make API request
status_code, data = alpha_v.make_base2_request("RETAIL_SALES")
logger.info(f'Received response from Alpha Vantage API: status code {status_code}')
logger.info(f"Data: {data['data'][:3]}")

# Transform data using pandas
records = data.get('data', [])
df = pd.DataFrame(records)

# Convert data types
df['date'] = pd.to_datetime(df['date'])
df['value'] = pd.to_numeric(df['value'], errors='coerce')

# Save data to database
engine = create_engine(
    f"mysql://{config['USER']}:{config['PASSWORD']}@localhost:3306/dbt_stage"
)
df.to_sql('retail_sales', con=engine, if_exists='replace', index=False)

logger.info("Data written to database successfully")
