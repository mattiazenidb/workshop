# Databricks notebook source
display(spark.read.table('dit_milan_external_synapse_catalog.cdc.captured_columns').limit(1))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dit_milan_catalog.curated_schema.synapse_curated_view AS
# MAGIC   SELECT
# MAGIC     object_id,
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('dit_demo') THEN 'REDACTED'
# MAGIC       ELSE column_name
# MAGIC     END AS column_name,
# MAGIC     column_id,
# MAGIC     column_type,
# MAGIC     column_ordinal,
# MAGIC     is_computed,
# MAGIC     masking_function
# MAGIC   FROM dit_milan_external_synapse_catalog.cdc.captured_columns

# COMMAND ----------

display(spark.read.table('dit_milan_catalog.curated_schema.synapse_curated_view').limit(1))
