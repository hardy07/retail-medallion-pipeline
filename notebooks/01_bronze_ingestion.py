# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# Read raw source table
raw_orders = spark.table("samples.tpch.orders")

# Add Bronze metadata — this is the key Bronze pattern: don't clean, just capture
bronze_orders = raw_orders.withColumn("_ingested_at", current_timestamp()) \
                           .withColumn("_source", lit("samples.tpch.orders"))

# Write to your own Bronze table
bronze_orders.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.bronze_orders")

display(bronze_orders)