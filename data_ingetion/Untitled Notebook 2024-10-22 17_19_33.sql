-- Databricks notebook source
-- MAGIC %python
-- MAGIC df = spark.read.format('json').load('dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source/orders/')
-- MAGIC display(df)

-- COMMAND ----------


