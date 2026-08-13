# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum, count, avg, date_trunc, year, month

silver_orders = spark.table("workspace.default.silver_orders")

# --- Gold table 1: Monthly revenue trend ---
gold_monthly_revenue = silver_orders \
    .groupBy(year("o_orderdate").alias("order_year"), month("o_orderdate").alias("order_month")) \
    .agg(
        _sum("o_totalprice").alias("total_revenue"),
        count("o_orderkey").alias("order_count"),
        avg("o_totalprice").alias("avg_order_value")
    ) \
    .orderBy("order_year", "order_month")

gold_monthly_revenue.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.gold_monthly_revenue")

# --- Gold table 2: Order status breakdown ---
gold_order_status = silver_orders \
    .groupBy("o_orderstatus") \
    .agg(
        count("o_orderkey").alias("order_count"),
        _sum("o_totalprice").alias("total_value")
    ) \
    .orderBy(col("order_count").desc())

gold_order_status.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.gold_order_status")

# --- Gold table 3: Data quality summary (feeds your "pipeline health" dashboard chart) ---
bronze_count = spark.table("workspace.default.bronze_orders").count()
silver_count = spark.table("workspace.default.silver_orders").count()
quarantine_count = spark.table("workspace.default.silver_orders_quarantine").count()

from pyspark.sql import Row
gold_pipeline_health = spark.createDataFrame([
    Row(layer="bronze", record_count=bronze_count),
    Row(layer="silver_valid", record_count=silver_count),
    Row(layer="silver_quarantined", record_count=quarantine_count),
])

gold_pipeline_health.write.format("delta").mode("overwrite") \
    .saveAsTable("workspace.default.gold_pipeline_health")

display(gold_monthly_revenue)
display(gold_order_status)

# COMMAND ----------

gold_monthly_revenue.toPandas().to_csv("/Volumes/workspace/default/gold_exports/gold_monthly_revenue.csv", index=False)
gold_order_status.toPandas().to_csv("/Volumes/workspace/default/gold_exports/gold_order_status.csv", index=False)
gold_pipeline_health.toPandas().to_csv("/Volumes/workspace/default/gold_exports/gold_pipeline_health.csv", index=False)