# Databricks notebook source
inventory = spark.read.table('dit_milan_catalog.curated_schema.inventory')

# COMMAND ----------

synapse_curated = spark.read.table('dit_milan_catalog.curated_schema.synapse_curated_view')

# COMMAND ----------

display(inventory.limit(1))

# COMMAND ----------

display(synapse_curated.limit(1))

# COMMAND ----------

joined = inventory.join(synapse_curated, inventory.id == synapse_curated.column_id)

# COMMAND ----------

display(joined)

# COMMAND ----------

joined.write.mode('overwrite').saveAsTable('dit_milan_catalog.curated_schema.warehouse_stocks_curated')
