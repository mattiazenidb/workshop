# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

warehouse_stocks_curated = spark.read.table(f"{user_name}.initial_schema.warehouse_stocks_curated")

# COMMAND ----------


warehouse_stocks_curated_features = warehouse_stocks_curated.select('userid').withColumn('price', round(rand()*(10-5)+5,0))

# COMMAND ----------

warehouse_stocks_curated_features.write.mode('overwrite').saveAsTable(f"{user_name}.initial_schema.warehouse_stocks_curated_features")
