# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

warehouse_stocks_curated = spark.read.table('dit_milan_catalog.curated_schema.warehouse_stocks_curated')

# COMMAND ----------


warehouse_stocks_curated_features = warehouse_stocks_curated.select('column_id').withColumn('price', round(rand()*(10-5)+5,0))


# COMMAND ----------

warehouse_stocks_curated_features.write.mode('overwrite').saveAsTable('dit_milan_catalog.curated_schema.warehouse_stocks_curated_features')
