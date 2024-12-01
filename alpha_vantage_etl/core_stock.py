#%%
import os
import sys
import yaml
from alpha_vantage import AlphaVantageWrapper
from logging_config import configure_logger

# Load configuration from YAML file
config = yaml.safe_load(open("config.yaml", "r"))

# # Set up Spark Session
# os.environ["PYSPARK_PYTHON"] = sys.executable
# os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Create Alpha Vantage wrapper
alpha_v = AlphaVantageWrapper(api_key=config['API_KEY'])
    
# Create logger 
logger = configure_logger()

#%%
function_name1 = "OVERVIEW"
function_name2 = "ETF_PROFILE"
other_parameter = "symbol=IBM"

#%%
status_code, overview = alpha_v.make_base3_request(function_name=function_name1, other_parameter=other_parameter)
status_code, etf_profile = alpha_v.make_base3_request(function_name=function_name1, other_parameter=other_parameter)

#%%
overview

# %%
etf_profile
#%%
