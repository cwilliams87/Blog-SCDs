-- Databricks notebook source
-- MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
-- MAGIC <h2>Type 3 - Add a previous value column</h2>
-- MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-3/</p>

-- COMMAND ----------

-- MAGIC %md ### Setup SCD Type 3 example table

-- COMMAND ----------

-- MAGIC %run "./~Configs/SCD-0-SetupSCD3"

-- COMMAND ----------

USE scd

-- COMMAND ----------

-- MAGIC %md ### Load employee table

-- COMMAND ----------

-- Select employees table (ordered by id column)
SELECT * FROM scd.scdType3
ORDER BY employee_id

-- COMMAND ----------

-- MAGIC %md ### Amend Country

-- COMMAND ----------

-- MAGIC %md <p>The employees <b>Maximo Moxon</b> (employee_id = 9), <b>Augueste Dimeloe</b> (employee_id = 10) and <b>Austina Wimbury</b> (employee_id = 11) have all relocated to an office in a different country.
-- MAGIC <br>
-- MAGIC <br>We want to amend the country values and create an accompanying column to display the previous ones.
-- MAGIC </p>

-- COMMAND ----------

-- Drop if already exists (for notebook continuity)
DROP VIEW IF EXISTS scdType3NEW;

-- Create View to merge
CREATE VIEW scdType3NEW AS SELECT 
col1 AS employee_id, 
col2 AS first_name,
col3 AS last_name,
col4 AS gender,
col5 AS address_country
FROM VALUES 
(9, 'Maximo', 'Moxon', 'Male', 'Canada'),
(10, 'Augueste', 'Dimeloe', 'Female', 'France'),
(11, 'Austina', 'Wimbury', 'Male', 'Germany');

-- Preview results
SELECT * FROM scdType3NEW

-- COMMAND ----------

-- MAGIC %md ### Merge tables
-- MAGIC <p>Insert if new, Update if already exists</p>

-- COMMAND ----------

-- MAGIC %md As the View containing new values has a different set of columns than the primary table, we use autoMerge for schema differences or evolution

-- COMMAND ----------

-- Set autoMerge to True
SET spark.databricks.delta.schema.autoMerge.enabled=true;

-- COMMAND ----------

-- Merge scdType3NEW dataset into existing
MERGE INTO scdType3
USING scdType3NEW

-- based on the following column(s)
ON scdType3.employee_id = scdType3NEW.employee_id

-- if there is a match do this...
WHEN MATCHED THEN 
  UPDATE SET scdType3.previous_country = scdType3.address_country, address_country = scdType3NEW.address_country
-- if there is no match insert new row
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ### Check Rows
-- MAGIC <p>Check row and value(s) have been updated</p>

-- COMMAND ----------

-- Check table for changed rows
SELECT * FROM scdType3
WHERE employee_id >= 9

-- COMMAND ----------

-- Check Delta history
DESCRIBE HISTORY scdType3

-- COMMAND ----------

-- Clean up
DROP VIEW IF EXISTS scdType3NEW
