# Databricks notebook source

filePath="dbfs:/FileStore/tables/GlobalRetail/bronze_layer/product_data/products.json"
df=spark.read.json(filePath)
df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new=df.withColumn("ingestion_timestamp",current_timestamp())
df_new.show()

# COMMAND ----------

spark.sql("use globalretail_bronze" )
df_new.write.format("delta").mode("append").saveAsTable("bronze_product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_product

# COMMAND ----------

import datetime

# path to the file you want to archive
filePath = "dbfs:/FileStore/tables/GlobalRetail/bronze_layer/product_data/products.json"

# archive folder
archive_folder = "dbfs:/FileStore/tables/GlobalRetail/bronze_layer/product_data/products/archive/"

# create archive file path with timestamp
archive_filepath = archive_folder + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".json"

# move file (not the whole folder)
dbutils.fs.mv(filePath, archive_filepath)

print("Archived file path:", archive_filepath)
