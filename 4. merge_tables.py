# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

inventory = spark.read.table(f"{user_name}.initial_schema.inventory")

# COMMAND ----------

synapse_curated = spark.read.table(f"{user_name}.curated_schema.synapse_curated_view")

# COMMAND ----------

display(inventory.limit(1))

# COMMAND ----------

display(synapse_curated.limit(1))

# COMMAND ----------

joined = inventory.join(synapse_curated, inventory.id == synapse_curated.userid)

# COMMAND ----------

display(joined)

# COMMAND ----------

joined.write.mode('overwrite').saveAsTable(f"{user_name}.curated_schema.warehouse_stocks_curated")
