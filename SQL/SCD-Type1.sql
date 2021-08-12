-- Databricks notebook source
-- MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
-- MAGIC <h2>Type 1 - Overwriting the value(s) or inserting a new row</h2>
-- MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-1/</p>

-- COMMAND ----------

-- MAGIC %md ### Setup SCD Type 1 example table

-- COMMAND ----------

-- MAGIC %run "./~Configs/SCD-0-SetupSCD1"

-- COMMAND ----------

USE scd

-- COMMAND ----------

-- MAGIC %md ### Load employee table

-- COMMAND ----------

-- Select employees table (ordered by id column)
SELECT * FROM scd.scdType1
ORDER BY employee_id

-- COMMAND ----------

-- MAGIC %md ### Amend Job Title

-- COMMAND ----------

-- MAGIC %md <p>The employee <b>Stu Sand</b> (employee_id = 2) has changed their job title from <b>Paralegal</b> to <b>Solicitor</b>.
-- MAGIC <br>We want to check if the row exists in the table, insert if new and overwrite if exists.
-- MAGIC </p>

-- COMMAND ----------

-- Drop if already exists (for notebook continuity)
DROP VIEW IF EXISTS scdType1NEW;

-- Create View to merge
CREATE VIEW scdType1NEW AS SELECT 
col1 AS employee_id, 
col2 AS first_name,
col3 AS last_name,
col4 AS gender, 
col5 AS address_street, 
col6 AS address_city, 
col7 AS address_country, 
col8 AS email, 
col9 AS job_title
FROM VALUES 
(2, 'Stu', 'Sand', 'Male', '83570 Fairview Way', 'Chicago', 'United States', 'ssand1@imdb.com', 'Solicitor');

-- Preview results
SELECT * FROM scdType1NEW

-- COMMAND ----------

-- MAGIC %md ### Merge tables
-- MAGIC <p>Insert if new, Update if already exists</p>

-- COMMAND ----------

-- Merge scdType1NEW dataset into existing
MERGE INTO scdType1
USING scdType1NEW

-- based on the following column(s)
ON scdType1.employee_id = scdType1NEW.employee_id

-- if there is a match do this...
WHEN MATCHED THEN UPDATE SET *
-- if there is no match insert new row
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ### Check Row
-- MAGIC <p>Check row and value(s) have been updated</p>

-- COMMAND ----------

-- Check table
SELECT * FROM scdType1
WHERE first_name = 'Stu' AND last_name = 'Sand'

-- COMMAND ----------

-- Check Delta history
DESCRIBE HISTORY scdType1

-- COMMAND ----------

-- Clean up
DROP VIEW IF EXISTS scdType1NEW
