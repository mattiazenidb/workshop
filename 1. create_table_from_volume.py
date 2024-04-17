# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

inventory = spark.read.csv('/Volumes/landing_zone/raw/raw_volume')

# COMMAND ----------

display(inventory)

# COMMAND ----------

inventory.count()

# COMMAND ----------

inventory.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f"{user_name}.initial_schema.inventory")
