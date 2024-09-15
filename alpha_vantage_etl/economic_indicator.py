from config import alpha_v, spark, logger, config

logger.info('Starting script')

# Make API request
status_code, data = alpha_v.make_base2_request("RETAIL_SALES")
logger.info(f'Received response from Alpha Vantage API: status code {status_code}')
logger.info(f"Data: {data['data'][:3]}")

# Transform data with Spark
records = data.get('data', [])
sdf = spark.createDataFrame(records)

sdf = sdf.withColumn("date", sdf.date.cast("date"))
sdf = sdf.withColumn("value", sdf.value.cast("int"))

# Save data to database
sdf.write.jdbc(url="jdbc:mysql://localhost:3306/dbt_stage", table="retail_sales",
               mode="overwrite", 
               properties={"user": config['USER'], "password": config['PASSWORD'], "driver": "com.mysql.cj.jdbc.Driver"})

spark.stop()