# Databricks notebook source
import urllib.request, json, requests
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Set GitHub URL
url = 'https://raw.githubusercontent.com/cwilliams87/Blog/main/07-2021/sampleEmployees'

# COMMAND ----------

def getSample():
  
  # Get Url
  resp = requests.get(url)
  jsonData = json.loads(resp.text)
  data = sc.parallelize(jsonData)
  
  # Set Schema
  schema = StructType(
    [
      StructField("id", LongType(), True),
      StructField("first_name", StringType(), True),
      StructField("last_name", StringType(), True),
      StructField("gender", StringType(), True),
      StructField("address_street", StringType(), True),
      StructField("address_city", StringType(), True),
      StructField("address_country", StringType(), True),
      StructField("email", StringType(), True),
      StructField("job_title", StringType(), True),
      StructField("start_date", StringType(), True),
      StructField("end_date", StringType(), True)
    ]
  )
  
  df = spark.createDataFrame(data, schema)
  
  # Convert Date Columns
  df = df.withColumn("start_date", col("start_date").cast(DateType()))
  df = df.withColumn("end_date", col("end_date").cast(DateType()))
  
  # Rename Column (for later SCD's)
  df = df.withColumnRenamed('id', 'employee_id')
  
  # Additional column for SCD Type 3
  df = df.withColumn('previous_country', lit(None).cast(StringType()))
  
  df.registerTempTable("EmployeeSample")
  
  return


# COMMAND ----------

getSample()
