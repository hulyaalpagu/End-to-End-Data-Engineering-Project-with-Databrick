# Databricks notebook source
spark.sql("use globalretail_silver ")
spark.sql("""
create table if not exists silver_orders(
    transaction_id string,
    customer_id string,
    product_id string,
    quantity int,
    total_amount double,
    transaction_date date,
    payment_method string,
    store_type string,
    order_status string,
    last_updated timestamp
) using delta
          """)

# COMMAND ----------

#Get the last processed timestamp from silver layer
last_processed_df=spark.sql("select max(last_updated) as last_processed from silver_orders")
last_processed_timestamp=last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp="1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

spark.sql(f"""
create or replace temporary view bronze_incremental_orders as
select * from myazuredatabricks.globalretail_bronze.bronze_transaction
where ingestion_timestamp>='{last_processed_timestamp}'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_incremental_orders

# COMMAND ----------

#Data Transformations:
#Quantity and total_amount normalization (setting negative values to 0)
#Date cating to ensure consistent date format
#order status derivation based on quantity and total_amount
#data quality checks:we filter out records with null transaction dates, customer IDs, or product IDs

# COMMAND ----------

spark.sql("""
create or replace temporary view silver_incremental_orders as
select transaction_id,
       customer_id,
       product_id,
       case when quantity<0 then 0 --Quantity normalization
            else quantity
       end as quantity,
       case when total_amount<0 then 0 --Total_amount normalization
            else total_amount
       end as total_amount,
       cast(transaction_date as date) as transaction_date, --Date cating to ensure consistent date format
       payment_method,
       store_type,
       case when quantity=0 or total_amount=0 then 'Cancelled' --order status derivation based on quantity and total_amount
            else 'Completed'
       end as order_status,
       current_timestamp() as last_updated
       from bronze_incremental_orders
       where transaction_date is not null
       and customer_id is not null
       and product_id is not null
      """)

# COMMAND ----------

spark.sql("""
merge into silver_orders target
using silver_incremental_orders source
on target.transaction_id=source.transaction_id
when matched then update set *
when not matched then insert *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_orders