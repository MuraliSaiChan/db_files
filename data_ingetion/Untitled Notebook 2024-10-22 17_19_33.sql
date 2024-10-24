-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df = spark.read.format('json').load('dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source/customers/')
-- MAGIC display(df.groupBy('state').agg(F.count('state').alias('count')).orderBy('count', ascending=False))

-- COMMAND ----------

use catalog hive_metastore;
use orders_custom_dlt;
show tables;

-- COMMAND ----------

select * from cdc_order;

-- COMMAND ----------


