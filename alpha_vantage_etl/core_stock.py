#%%
from config import alpha_v, spark, logger, config

logger.info('Starting script')

#%%
function_name = "OVERVIEW"
other_parameter = "symbol=IBM"
status_code, data1 = alpha_v.make_base3_request(function_name=function_name, other_parameter=other_parameter)
# %%
columns = list(data1.keys())
sdf1 = spark.createDataFrame([data1], schema=columns)
# %%
sdf1.show()
# %%
# Assuming `data1` is the dictionary you receive from the API
function_name = "OVERVIEW"
other_parameter = "symbol=IBM"
status_code, data1 = alpha_v.make_base3_request(function_name=function_name, other_parameter=other_parameter)

# Convert the dictionary into a list of tuples to maintain the order
data_list = [(k, v) for k, v in data1.items()]

# Create the DataFrame with specified column names
sdf1 = spark.createDataFrame(data_list, ["Key", "Value"])

# Show the DataFrame in its original order
sdf1.show()

#%%