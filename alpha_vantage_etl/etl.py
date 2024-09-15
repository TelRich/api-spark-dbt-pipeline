# Import necessary libraries
import os
import sys
from dotenv import load_dotenv  # For loading environment variables
from pyspark.sql import SparkSession  # For Spark DataFrame operations
import json  # For handling JSON data
from pyspark.sql import Row  # For creating rows of data
import requests  # For making HTTP requests

# Load environment variables from a .env file
load_dotenv()  

# Set the Python executable for PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Define the MySQL connection URL and properties
mysql_url = "jdbc:mysql://localhost:3306/dbt_stage"  # MySQL database URL
mysql_properties = {
    "user": f"{os.getenv('USER')}",  # Database username from environment variable
    "password": f"{os.getenv('PASSWORD')}",  # Database password from environment variable
    "driver": "com.mysql.cj.jdbc.Driver"  # MySQL JDBC driver
}

# Prepare the API request URL to get retail sales data
url = f"https://www.alphavantage.co/query?function=RETAIL_SALES&apikey={os.getenv('API_KEY')}"
r = requests.get(url)  # Send a GET request to the API

# Parse the JSON response from the API
data = r.json()  # Convert the response to a JSON object
records = data.get('data', [])  # Extract the 'data' field; default to an empty list if not found

# Optionally save the JSON data to a file (commented out)
# with open('retail_sales_data.json', 'w') as json_file:
#     json.dump(data, json_file, indent=4)  # Use indent=4 for pretty-printing

# Create a Spark session for processing data
spark = SparkSession.builder \
        .appName("JSON to MySQL") \
        .getOrCreate()  # Initialize the Spark session

# Create a Spark DataFrame from the records list
df = spark.createDataFrame(records)

# Cast 'date' column to a date type and 'value' to an integer type
df = df.withColumn("date", df.date.cast('date'))  
df = df.withColumn("value", df.value.cast('int'))

# Optionally show the DataFrame for debugging (commented out)
# df.show()  

# Write the DataFrame to the MySQL database 'retail_sales' table
df.write.jdbc(url=mysql_url, table='retail_sales', mode='overwrite', properties=mysql_properties)

# Stop the Spark session after the operations are complete
spark.stop()
