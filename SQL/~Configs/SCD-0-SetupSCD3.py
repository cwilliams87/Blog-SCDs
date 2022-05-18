# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS scd.scdType3;
# MAGIC CREATE TABLE scd.scdType3 USING delta AS
# MAGIC SELECT employee_id, first_name, last_name, gender, address_country FROM scd.employees
# MAGIC ORDER BY employee_id
