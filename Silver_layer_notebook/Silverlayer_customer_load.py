# Databricks notebook source
spark.sql("use globalretail_silver")
spark.sql(""" 
          create table if not exists silver_customers(
              customer_id string,
              name string,
              email string,
              country string,
              customer_type string,
              registration_date date,
              age int,
              gender string,
              total_purchases int,
              customer_segment string,
              days_since_registeration int,
              last_updated timestamp) 
          """)

# COMMAND ----------

#Get The Last processed timestamp from silver layer
last_processed_df=spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_customers")
last_processed_timestamp=last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp="1900-01-01T00:00:00.000+00:00"

# COMMAND ----------

#Create a temporary view of incremental bronze data
spark.sql(f"""
          create or replace temporary view bronze_incremental AS
          select * from myazuredatabricks.globalretail_bronze.bronze_customer c
          where c.ingestion_timestamp>'{last_processed_timestamp}'
          """)

# COMMAND ----------

display(spark.sql("select * from bronze_incremental limit 10"))

# COMMAND ----------

#Validate email address (null or not null)
#Valid age between 18 to 100
#Create customer_segment as total_purchases>10000 then 'High Value' if >5000 then 'Medium Value' else 'low Value'
#days since user is registred in the system
#Remove any junk records where total_purchase is negative number

# COMMAND ----------

spark.sql("""
create or replace temporary view silver_incremental as
select 
  customer_id,
  name,
  email,
  country,
  customer_type,
  registration_date,
  age,
  gender,
  total_purchases,
  case
    when total_purchases > 1000 then 'High Value'
    when total_purchases > 500 then 'Medium Value'
    else 'Low Value'
  end as customer_segment,
  datediff(current_date(), registration_date) as days_since_registeration, -- days since user is registered in system
  current_timestamp() as last_updated
from bronze_incremental
where age between 18 and 100 -- valid age between 18 and 100
      and email is not null -- only valid email address is not null
      and total_purchases >= 0 -- remove any junk records where total_purchase is negative number
""")


# COMMAND ----------

display(spark.sql("select * from silver_incremental"))

# COMMAND ----------

spark.sql("""
          merge into silver_customers target
          using silver_incremental source
          on target.customer_id=source.customer_id
          when matched then update set *
          when not matched then insert *
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_customers