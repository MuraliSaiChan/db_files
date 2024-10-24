# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

@dlt.table(comment = 'reading stream from cloud files', table_properties={'quality':'bronze'})
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.inferColumnTypes", "true").load("dbfs:/mnt/dbacademy-users/saichand000@hotmail.com/data-engineer-learning-path/pipeline_demo/stream-source/customers/")
    )

# COMMAND ----------

@dlt.table(comment = 'silver table for customers from state CA', table_properties={'quality':'silver'})
@dlt.expect_or_drop('from_state_ca',"STATE = 'CA'")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
    )

# COMMAND ----------

dlt.create_streaming_table(name = 'customers_cdc')

dlt.apply_changes(
target = 'customers_cdc',
source = 'silver_customers',
keys = ['customer_id'],
sequence_by = F.col('timestamp'),
stored_as_scd_type = 2
)

# COMMAND ----------

@dlt.table(comment = 'silver table for orders from state CA', table_properties={'quality':'silver'})
def customer_orders_ca():
    return (dlt.read('silver_customers').join(dlt.read('silver_orders'), on = 'customer_id')
            .select('customer_id', 'order_id', 'email', 'name', 'order_timestamp'))
