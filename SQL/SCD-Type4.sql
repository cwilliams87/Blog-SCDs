-- Databricks notebook source
-- MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
-- MAGIC <h2>Type 4 - Using the Delta History</h2>
-- MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-4/</p>

-- COMMAND ----------

-- MAGIC %md ### Setup SCD Type 4 example table

-- COMMAND ----------

-- MAGIC %run "./~Configs/SCD-0-SetupSCD4"

-- COMMAND ----------

USE scd

-- COMMAND ----------

-- MAGIC %md ### Load employee table

-- COMMAND ----------

-- Select employees table (ordered by id column)
SELECT * FROM scd.scdType4
ORDER BY employee_id

-- COMMAND ----------

-- MAGIC %md ### Amend Email Address and Name

-- COMMAND ----------

-- MAGIC %md <p>The employees <b>Rae Maith</b> (employee_id = 1) and <b>Stu Sand</b> (employee_id = 2) have changes to be made.
-- MAGIC <br><b>Rae</b> needs a change of  email address whereas <b>Stu</b> only shows a shortened version of his name which should be stuart.
-- MAGIC <br>
-- MAGIC <br>We can make the changes using a similar approach to Type 1 SCD and use the Delta History to TIME TRAVEL to historical versions. This will create the functionality similar to a traditional Type 4 Slowly Changing Dimension e.g. active dataset and history table.
-- MAGIC </p>

-- COMMAND ----------

-- Drop if already exists (for notebook continuity)
DROP VIEW IF EXISTS scdType4NEW;

-- Create View to merge
CREATE VIEW scdType4NEW AS SELECT 
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
( 1, 'Rae', 'Maith', 'Male', '6877 Roth Hill', 'Sioux City', 'United States', 'rmaith0@wisc.org', 'VP Quality Control'),
( 2, 'Stuart', 'Sand', 'Male', '83570 Fairview Way', 'Chicago', 'United States', 'ssand1@imdb.com', 'Paralegal');

-- Preview results
SELECT * FROM scdType4NEW

-- COMMAND ----------

-- MAGIC %md ### Merge tables
-- MAGIC <p>Insert if new, Update if already exists</p>

-- COMMAND ----------

-- Merge scdType1NEW dataset into existing
MERGE INTO scdType4
USING scdType4NEW

-- based on the following column(s)
ON scdType4.employee_id = scdType4NEW.employee_id

-- if there is a match do this...
WHEN MATCHED THEN 
  UPDATE SET *
-- if there is no match insert new row
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ### Check Row
-- MAGIC <p>Check row and value(s) have been updated</p>

-- COMMAND ----------

-- Check table for changed rows
SELECT * FROM scdType4

-- COMMAND ----------

-- Check Delta history
DESCRIBE HISTORY scdType4

-- COMMAND ----------

-- MAGIC %md ### View previous version of table

-- COMMAND ----------

-- View current and previous versions
SELECT 0 AS version, *  FROM scdType4 VERSION AS OF 0
WHERE employee_id = 1 OR employee_id = 2
UNION ALL
SELECT 1 AS version, * FROM scdType4 VERSION AS OF 1
WHERE employee_id = 1 OR employee_id = 2

-- COMMAND ----------

-- MAGIC %md ### Rollback to previous versions

-- COMMAND ----------

-- The table can be restored to a previous version
RESTORE TABLE scdType4 TO VERSION AS OF 0

-- COMMAND ----------

SELECT * FROM scdType4

-- COMMAND ----------

-- Clean up
DROP VIEW IF EXISTS scdType4NEW
