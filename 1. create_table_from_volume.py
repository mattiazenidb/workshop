# Databricks notebook source
inventory = spark.read.csv('/Volumes/dit_milan_catalog/raw_schema/raw')

# COMMAND ----------

display(inventory)

# COMMAND ----------

inventory.count()

# COMMAND ----------

inventory.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('dit_milan_catalog.curated_schema.inventory')
