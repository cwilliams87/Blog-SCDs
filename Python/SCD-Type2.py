# Databricks notebook source
# MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
# MAGIC <h2>Type 2 - Add a new row (with active row indicators)</h2>
# MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/</p>

# COMMAND ----------

# MAGIC %md ### Setup SCD Type 2 example table

# COMMAND ----------

# MAGIC %run "./~Configs/SCD-0-SetupSCD2"

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
scdType2DF = spark.table("scd.scdType2")

# COMMAND ----------

# Display dataframe
display(scdType2DF.orderBy("employee_id"))

# COMMAND ----------

# MAGIC %md We can see that all employee records are active as they contain nulls for the end_date.

# COMMAND ----------

# Check to see which rows are inactive (have an end date)
display(scdType2DF.where("end_date <> null").orderBy("employee_id"))

# COMMAND ----------

# MAGIC %md ### Amend Address and Last Name
# MAGIC (and end_date)

# COMMAND ----------

# MAGIC %md <p>The employees <b>Fred Flintoff</b> (employee_id = 6) and <b>Hilary Casillis</b> (employee_id = 21) have changes to be made.
# MAGIC <br><b>Fred</b> needs a change of address whereas <b>Hilary</b> has recently got married and will be changing her last_name.
# MAGIC <br>
# MAGIC <br>We want to create new rows for these updated records and add an end_date to signifiy that the old rows have now expired.
# MAGIC <br><i>Note: An additional Active Flag column could also be created to show the currently active records in the table.</i>
# MAGIC </p>

# COMMAND ----------

# Create dataset
dataForDF = [
(None, 6, 'Fred', 'Flintoff', 'Male', '3686 Dogwood Road', 'Phoenix', 'United States', 'fflintoft5@unblog.fr', 'Financial Advisor'),
(None, 21, 'Hilary', 'Thompson', 'Female', '4 Oxford Pass', 'San Diego', 'United States', 'hcasillisk@washington.edu', 'Senior Sales Associate')
]

# Create Schema structure
schema = StructType([
  StructField("id", IntegerType(), True),
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
scd2Temp = spark.createDataFrame(dataForDF, schema)

# Preview dataset
display(scd2Temp)

# COMMAND ----------

# MAGIC %md ### Merge tables
# MAGIC <p>Insert if new, Update if already exists</p>

# COMMAND ----------

# Create list of selected employee_id's
empList = scd2Temp.select(collect_list(scd2Temp['employee_id'])).collect()[0][0]

# Select columns in new dataframe to merge
scdChangeRows = scd2Temp.selectExpr(
  "null AS id", "employee_id", "first_name", "last_name", "gender", "address_street", "address_city", "address_country", "email", "job_title", "current_date AS start_date", "null AS end_date"
)

# Union join queries to match incoming rows with existing
scdChangeRows = scdChangeRows.unionByName(
  scdType2DF  
  .where(col("employee_id").isin(empList)), allowMissingColumns=True
)
# Preview results
display(scdChangeRows)

# COMMAND ----------

# Convert table to Delta
deltaTable = DeltaTable.forName(spark, "scdType2")

# Merge Delta table with new dataset
(
  deltaTable
    .alias("original2")
    # Merge using the following conditions
    .merge( 
      scdChangeRows.alias("updates2"),
      "original2.id = updates2.id"
    )
    # When matched UPDATE ALL values
    .whenMatchedUpdate(
    set={
      "original2.end_date" : current_date()
    }
    )
    # When not matched INSERT ALL rows
    .whenNotMatchedInsertAll()
    # Execute
    .execute()
)

# COMMAND ----------

scdType2DF.selectExpr( 
  "ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id", "employee_id", "first_name", "last_name", "gender", "address_street", "address_city", "address_country", 
  "email", "job_title", "start_date", "end_date"
).write.insertInto(tableName="scdType2", overwrite=True)

# COMMAND ----------

# MAGIC %md ### Check Row
# MAGIC <p>Check row and value(s) have been updated</p>

# COMMAND ----------

display(
  sql("SELECT * FROM scdType2 WHERE employee_id = 6 OR employee_id = 21")
)

# COMMAND ----------

# Check Delta history
sql("DESCRIBE HISTORY scdType2")
