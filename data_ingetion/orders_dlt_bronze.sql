-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source

-- COMMAND ----------

create or refresh streaming live table bronze_orders
as select *, current_timestamp() as processed_at from cloud_files('dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source/orders/', 'json', map('cloudFiles.inferColumnTypes','true'))

-- COMMAND ----------

create or refresh streaming live table silver_orders
(constraint notified_records_only expect (notifications = 'Y') on violation drop row)
comment "filtering bronze table on  notification column"
tblproperties('quality' = 'silver')
as select * from STREAM(LIVE.bronze_orders)

-- COMMAND ----------

create or refresh live table gold_orders
as select customer_id, count(customer_id) as count_orders from LIVE.silver_orders group by customer_id;

-- COMMAND ----------

create streaming live table cdc_order;

apply changes into LIVE.cdc_order from STREAM(LIVE.silver_orders) keys(customer_id) sequence by order_timestamp
stored as scd type 2;

-- COMMAND ----------


