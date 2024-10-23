-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source

-- COMMAND ----------

create or refresh streaming live table orders_bronze
as select *, current_timestamp() as processed_at from cloud_files('dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source/orders/', 'json', map('cloudFiles.inferColumnTypes','true'))

-- COMMAND ----------

create or refresh streaming live table orders_silver
(constraint notified_records_only expect (notifications = 'Y') on violation drop row)
comment "filtering bronze table on  notification column"
tblproperties('quality' = 'silver')
as select * from STREAM(LIVE.orders_bronze)

-- COMMAND ----------

create or refresh live table orders_gold
as select count(customer_id) from LIVE.orders_silver group by customer_id;

-- COMMAND ----------


