# Databricks notebook source
spark.sql("use myazuredatabricks.globalretail_gold")
spark.sql("""
CREATE OR REPLACE TABLE gold_customer_order_product as
select 
c.customer_id,
    c.name AS customer_name,
    c.email,
    c.country,
    c.customer_type,
    c.age,
    c.gender,
    c.customer_segment,
    c.total_purchases,
    c.days_since_registeration,
    o.transaction_id,
    o.product_id,
    o.quantity,
    o.total_amount,
    o.transaction_date,
    o.payment_method,
    o.store_type,
    o.order_status,
    p.name AS product_name,
    p.category AS product_category,
    p.brand AS product_brand,
    p.price AS product_price,
    p.rating AS product_rating,
    p.price_category,
    p.stock_status

from myazuredatabricks.globalretail_silver.silver_customers as c
left join myazuredatabricks.globalretail_silver.silver_orders as o on c.customer_id=o.customer_id
left join myazuredatabricks.globalretail_silver.silver_products as p on o.product_id=p.product_id
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_customer_order_product