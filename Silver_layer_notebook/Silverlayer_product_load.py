# Databricks notebook source
spark.sql("use globalretail_silver")
spark.sql(""" 
          create table if not exists silver_products(
              product_id string,
              name string,
              category string,
              brand string,
              price double,
              stock_quantity int,
              rating double,
              is_active boolean,
              price_category string,
              stock_status string,
              last_updated timestamp) using delta
          """)

# COMMAND ----------

#Get The Last processed timestamp from silver layer
last_processed_df=spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_products")
last_processed_timestamp=last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp="1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

#Create a temporary view of incremental bronze data
spark.sql(f"""
          create or replace temporary view bronze_incremental_products AS
          select * from myazuredatabricks.globalretail_bronze.bronze_product c
          where c.ingestion_timestamp>'{last_processed_timestamp}'
          """)

# COMMAND ----------

#Data Transformation
# Price normalization (setting negative prices to 0)
#Stock quatity normalization (setting negative prices to 0)
#Rating normalization (clamping between 0 and 5)
#Price categorization (Premium, Standard, Budget)
#Stock status calculation (Out of Stock, Low Stock, Moderate Stock,Sufficient Stock)

# COMMAND ----------

spark.sql("""
          create or replace temporary view silver_incremental_products as
          select
          product_id,
          name,
          category,
          brand,
          case when price<0 then 0 --Price normalization
               else price
          end as price,
          case when stock_quantity<0 then 0 --Stock quatity normalization
               else stock_quantity
          end as stock_quantity,
          case when rating<0 then 0 --Rating normalization 
               when rating>5 then 5
               else rating end as rating,
          is_active,
          case when price>1000 then 'Premium' --Price categorization
               when price>100 then 'Standard'
               else 'Budget'
          end as price_category,
          case when stock_quantity=0 then 'Out of Stock' --Stock status calculation
               when stock_quantity<10 then 'Low Stock'
               when stock_quantity<50 then 'Moderate Stock'
               else 'Sufficient Stock'
          end as stock_status,
          current_timestamp() as last_updated
          from bronze_incremental_products
          where name is not null and category is not null
          """)

# COMMAND ----------

spark.sql("""
Merge into silver_products target
using silver_incremental_products source
on target.product_id=source.product_id
when matched then update set *
when not matched then insert *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myazuredatabricks.globalretail_silver.silver_products