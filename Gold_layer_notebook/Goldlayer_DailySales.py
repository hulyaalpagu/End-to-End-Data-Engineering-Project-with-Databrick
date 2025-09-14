# Databricks notebook source
spark.sql("use globalretail_gold")
spark.sql("""
          create or replace table gold_daily_sales as 
          select 
             transaction_date,
             sum(total_amount) as daily_total_sales
          from myazuredatabricks.globalretail_silver.silver_orders
          group by transaction_date
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_daily_sales