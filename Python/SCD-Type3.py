# Databricks notebook source
# MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
# MAGIC <h2>Type 3 - Add a previous value column</h2>
# MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-3/</p>

# COMMAND ----------

# MAGIC %md ### Setup SCD Type 3 example table

# COMMAND ----------

# MAGIC %run "./~Configs/SCD-0-SetupSCD3"

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
scdType3DF = spark.table("scd.scdType3")

# COMMAND ----------

# Display dataframe
display(scdType3DF.orderBy("employee_id"))

# COMMAND ----------

# MAGIC %md ### Amend Country

# COMMAND ----------

# MAGIC %md <p>The employees <b>Maximo Moxon</b> (employee_id = 9), <b>Augueste Dimeloe</b> (employee_id = 10) and <b>Austina Wimbury</b> (employee_id = 11) have all relocated to an office in a different country.
# MAGIC <br>
# MAGIC <br>We want to amend the country values and create an accompanying column to display the previous ones.
# MAGIC </p>

# COMMAND ----------

# Create dataset
dataForDF = [
(9, 'Maximo', 'Moxon', 'Male', 'Canada'),
(10, 'Augueste', 'Dimeloe', 'Female', 'France'),
(11, 'Austina', 'Wimbury', 'Male', 'Germany')
]

# Create Schema structure
schema = StructType([
  StructField("employee_id", IntegerType(), True),
  StructField("first_name", StringType(), True),
  StructField("last_name", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("address_country", StringType(), True)
])

# Create as Dataframe
scd3Temp = spark.createDataFrame(dataForDF, schema)

# Preview dataset
display(scd3Temp)

# COMMAND ----------

# MAGIC %md ### Merge tables
# MAGIC <p>Insert if new, Update if already exists</p>

# COMMAND ----------

# MAGIC %md As the View containing new values has a different set of columns than the primary table, we use autoMerge for schema differences or evolution

# COMMAND ----------

# Set autoMerge to True
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled ","true")

# COMMAND ----------

# Convert table to Delta
deltaTable = DeltaTable.forName(spark, "scdType3")

# Merge Delta table with new dataset
(
  deltaTable
    .alias("original3")
    # Merge using the following conditions
    .merge( 
      scd3Temp.alias("updates3"),
      "original3.employee_id = updates3.employee_id"
    )
    # When matched UPDATE ALL values
    .whenMatchedUpdate(
    set={
      "original3.previous_country" : "original3.address_country",
      "original3.address_country" : "updates3.address_country"
    }
    )
    # When not matched INSERT ALL rows
    .whenNotMatchedInsertAll()
    # Execute
    .execute()
)

# COMMAND ----------

# MAGIC %md ### Check Rows
# MAGIC <p>Check row and value(s) have been updated</p>

# COMMAND ----------

# Check table for changed rows
display(sql("SELECT * FROM scdType3 WHERE employee_id >= 9"))

# COMMAND ----------

# Check Delta history
display(sql("DESCRIBE HISTORY scdType3"))
