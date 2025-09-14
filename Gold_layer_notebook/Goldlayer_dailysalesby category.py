# Databricks notebook source
spark.sql("use globalretail_gold")
spark.sql("""
create or replace  table gold_category_sales AS
select 
p.category as product_category,
sum(o.total_amount) as category_total_sales
from myazuredatabricks.globalretail_silver.silver_orders o
join myazuredatabricks.globalretail_silver.silver_products p
on o.product_id=p.product_id
group by p.category
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_category_sales