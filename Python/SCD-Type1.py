# Databricks notebook source
# MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
# MAGIC <h2>Type 1 - Overwriting the value(s) or inserting a new row</h2>
# MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-1/</p>

# COMMAND ----------

# MAGIC %md ### Setup SCD Type 1 example table

# COMMAND ----------

# MAGIC %run "./~Configs/SCD-0-SetupSCD1"

# COMMAND ----------

# Import dependencies
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Set default database
spark.catalog.setCurrentDatabase("scd")

# COMMAND ----------

# MAGIC %md ### Load employee table

# COMMAND ----------

# Create dataframe from HIVE db scd
scdType1DF = spark.table("scd.scdType1")

# COMMAND ----------

# Display dataframe
display(scdType1DF.orderBy("employee_id"))

# COMMAND ----------

# MAGIC %md ### Amend Job Title

# COMMAND ----------

# MAGIC %md <p>The employee <b>Stu Sand</b> (employee_id = 2) has changed their job title from <b>Paralegal</b> to <b>Solicitor</b>.
# MAGIC <br>We want to check if the row exists in the table, insert if new and overwrite if exists.
# MAGIC </p>

# COMMAND ----------

# Create dataset
dataForDF = [
(2, 'Stu', 'Sand', 'Male', '83570 Fairview Way', 'Chicago', 'United States', 'ssand1@imdb.com', 'Solicitor')
]

# Create Schema structure
schema = StructType([
  StructField("employee_id", IntegerType(), True),
  StructField("first_name", StringType(), True),
  StructField("last_name", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("address_street", StringType(), True),
  StructField("address_city", StringType(), True),
  StructField("address_country", StringType(), True),
  StructField("email", StringType(), True),
  StructField("job_title", StringType(), True)
])

# Create as Dataframe
scd1Temp = spark.createDataFrame(dataForDF, schema)

# Preview dataset
display(scd1Temp)

# COMMAND ----------

# MAGIC %md ### Merge tables
# MAGIC <p>Insert if new, Update if already exists</p>

# COMMAND ----------

# Convert table to Delta
deltaTable = DeltaTable.forName(spark, "scdType1")

# Merge Delta table with new dataset
(
  deltaTable
    .alias("original1")
    # Merge using the following conditions
    .merge( 
      scd1Temp.alias("updates1"),
      "original1.employee_id = updates1.employee_id"
    )
    # When matched UPDATE ALL values
    .whenMatchedUpdateAll()
    # When not matched INSERT ALL rows
    .whenNotMatchedInsertAll()
    # Execute
    .execute()
)

# COMMAND ----------

# MAGIC %md ### Check Row
# MAGIC <p>Check row and value(s) have been updated</p>

# COMMAND ----------

# Check 
display(
  deltaTable.toDF().orderBy("employee_id").where("first_name = 'Stu' AND last_name = 'Sand'")
)

# COMMAND ----------

# Check Delta History
display(deltaTable.history())
