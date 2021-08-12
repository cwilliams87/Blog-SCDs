# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS scd.scdType1;
# MAGIC CREATE TABLE scd.scdType1 USING delta AS
# MAGIC SELECT employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title FROM scd.employees
