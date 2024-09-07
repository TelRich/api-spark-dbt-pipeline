#%%
import os
import sys
from pyspark.sql import SparkSession
import json
from pyspark.sql import Row
import requests

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Replace with your actual API key
url = 'https://www.alphavantage.co/query?function=RETAIL_SALES&apikey=VDSZ89HER4WUE7NO'
r = requests.get(url)

# Parse the JSON response
data = r.json()

# Save the JSON data to a file
with open('retail_sales_data.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)  # Use indent=4 for a pretty-print format

#%%
spark = SparkSession.builder \
        .appName("JSON to MySQL") \
        .getOrCreate()
# %%
records = data.get('data', [])
# %%
df = spark.createDataFrame(records)

# %%
df.show()
#%%
spark.stop()
