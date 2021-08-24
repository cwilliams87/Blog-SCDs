-- Databricks notebook source
-- MAGIC %md <h1>Slowly Changing Dimensions with Delta</h1>
-- MAGIC <h2>Type 2 - Add a new row (with active row indicators)</h2>
-- MAGIC <p>https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/</p>

-- COMMAND ----------

-- MAGIC %md ### Setup SCD Type 2 example table

-- COMMAND ----------

-- MAGIC %run "./~Configs/SCD-0-SetupSCD2"

-- COMMAND ----------

USE scd

-- COMMAND ----------

-- MAGIC %md ### Load employee table

-- COMMAND ----------

-- Select employees table (ordered by id column)
SELECT * FROM scd.scdType2
ORDER BY employee_id

-- COMMAND ----------

-- MAGIC %md We can see that all employee records are active as they contain nulls for the end_date.

-- COMMAND ----------

-- Check to see which rows are inactive (have an end date)
SELECT * FROM scd.scdType2
WHERE end_date <> null

-- COMMAND ----------

-- MAGIC %md ### Amend Address and Last Name
-- MAGIC (and end_date)

-- COMMAND ----------

-- MAGIC %md <p>The employees <b>Fred Flintoff</b> (employee_id = 6) and <b>Hilary Casillis</b> (employee_id = 21) have changes to be made.
-- MAGIC <br><b>Fred</b> needs a change of address whereas <b>Hilary</b> has recently got married and will be changing her last_name.
-- MAGIC <br>
-- MAGIC <br>We want to create new rows for these updated records and add an end_date to signifiy that the old rows have now expired.
-- MAGIC <br><i>Note: An additional Active Flag column could also be created to show the currently active records in the table.</i>
-- MAGIC </p>

-- COMMAND ----------

-- Drop if already exists (for notebook continuity)
DROP VIEW IF EXISTS scdType2NEW;

-- Create View to merge
CREATE VIEW scdType2NEW AS SELECT 
col1 AS id,
col2 AS employee_id, 
col3 AS first_name,
col4 AS last_name,
col5 AS gender, 
col6 AS address_street, 
col7 AS address_city, 
col8 AS address_country, 
col9 AS email, 
col10 AS job_title
FROM VALUES 
(5, 6, 'Fred', 'Flintoff', 'Male', '3686 Dogwood Road', 'Phoenix', 'United States', 'fflintoft5@unblog.fr', 'Financial Advisor'),
(20, 21, 'Hilary', 'Thompson', 'Female', '4 Oxford Pass', 'San Diego', 'United States', 'hcasillisk@washington.edu', 'Senior Sales Associate');

-- Preview results
SELECT * FROM scdType2NEW

-- COMMAND ----------

-- MAGIC %md ### Merge tables
-- MAGIC <p>Insert if new, Update if already exists</p>

-- COMMAND ----------

-- Example ChangeRows table
SELECT 
  null AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, current_date AS start_date, null AS end_date
FROM scdType2NEW
UNION ALL
SELECT 
  id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date 
FROM scdType2
WHERE employee_id IN 
(SELECT employee_id FROM scdType2NEW)

-- COMMAND ----------

-- Merge scdType2NEW dataset into existing
MERGE INTO scdType2
USING 

-- Update table with rows to match (new and old referenced)
(
SELECT 
  null AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, current_date AS start_date, null AS end_date
FROM scdType2NEW
UNION ALL
SELECT 
  id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date 
FROM scdType2
WHERE employee_id IN 
(SELECT employee_id FROM scdType2NEW)
) scdChangeRows

-- based on the following column(s)
ON scdType2.id = scdChangeRows.id

-- if there is a match do this...
WHEN MATCHED THEN 
  UPDATE SET scdType2.end_date = current_date()
-- if there is no match insert new row
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- Order nulls in DF to the end and recreate row numbers (as delta does not currently support auto incrementals)
INSERT OVERWRITE scdType2
SELECT 
ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date
FROM scdType2

-- COMMAND ----------

-- MAGIC %md ### Check Row
-- MAGIC <p>Check row and value(s) have been updated</p>

-- COMMAND ----------

-- Check table for changed rows
SELECT * FROM scdType2
WHERE employee_id = 21 OR employee_id =6

-- COMMAND ----------

-- Check Delta history
DESCRIBE HISTORY scdType2

-- COMMAND ----------

-- Clean up
DROP VIEW IF EXISTS scdType2NEW
