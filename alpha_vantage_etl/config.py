import os
import sys
import yaml
from pyspark.sql import SparkSession
from alpha_vantage import AlphaVantageWrapper
from logging_config import configure_logger

# Load configuration from YAML file
config = yaml.safe_load(open("config.yaml", "r"))

# Set up Spark Session
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Create Alpha Vantage wrapper
alpha_v = AlphaVantageWrapper(api_key=config['API_KEY'])

# Create Spark Session
spark = SparkSession.builder \
    .appName("API to MySQL DB") \
    .getOrCreate() 
    
# Create logger 
logger = configure_logger()