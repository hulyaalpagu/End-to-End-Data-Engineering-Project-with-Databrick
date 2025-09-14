# Databricks notebook source

filePath="dbfs:/FileStore/tables/GlobalRetail/bronze_layer/transaction/transaction_snappy.parquet"
df=spark.read.parquet(filePath)
df.show()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col
new_df=df.withColumn("transaction_date",to_timestamp(col("transaction_date")))
new_df.printSchema()
display(new_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_new=new_df.withColumn("ingestion_timestamp",current_timestamp())
df_new.show()

# COMMAND ----------

spark.sql("use globalretail_bronze" )
df_new.write.format("delta").mode("append").saveAsTable("bronze_transaction")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_transaction

# COMMAND ----------

import datetime

# path to the file you want to archive
filePath = "dbfs:/FileStore/tables/GlobalRetail/bronze_layer/transaction/transaction_snappy.parquet"

# archive folder
archive_folder = "dbfs:/FileStore/tables/GlobalRetail/bronze_layer/transaction/transaction_snappy/archive/"

# create archive file path with timestamp
archive_filepath = archive_folder + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".parquet"

# move file (not the whole folder)
dbutils.fs.mv(filePath, archive_filepath)

print("Archived file path:", archive_filepath)
