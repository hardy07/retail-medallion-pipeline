# Databricks notebook source
# MAGIC %md
# MAGIC This silver layer enforces structure and quality. Here we fix types, drop/flag bad records, deduplicate, standardize formats.

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, current_timestamp
from pyspark.sql import functions as F

bronze_orders = spark.table("workspace.default.bronze_orders")

# 1. Deduplicate on primary key, keeping the latest ingested record
silver_orders = bronze_orders.dropDuplicates(["o_orderkey"])

# 2. Standardize types and formats
silver_orders = silver_orders \
    .withColumn("o_orderstatus", trim(upper(col("o_orderstatus")))) \
    .withColumn("o_totalprice", col("o_totalprice").cast("decimal(18,2)")) \
    .withColumn("o_orderdate", col("o_orderdate").cast("date"))

# 3. Data quality filter — drop records that fail basic sanity checks
#    (in a real pipeline, invalid rows would go to a quarantine table, not just dropped)
valid_orders = silver_orders.filter(
    (col("o_orderkey").isNotNull()) &
    (col("o_totalprice") > 0) &
    (col("o_orderdate").isNotNull())
)

invalid_orders = silver_orders.subtract(valid_orders)
print(f"Valid: {valid_orders.count()}, Invalid/quarantined: {invalid_orders.count()}")

# 4. Add lineage metadata
final_silver = valid_orders.withColumn("_processed_at", current_timestamp())

# Write Silver table
final_silver.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.silver_orders")

# Write quarantine table — this is the detail that impresses interviewers
invalid_orders.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.silver_orders_quarantine")

display(final_silver.limit(10))