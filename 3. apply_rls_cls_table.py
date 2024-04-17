# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dit_milan_catalog.curated_schema.inventory

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION dit_milan_catalog.curated_schema.rls_function (area STRING) RETURN IF(is_account_group_member('dit_group'), area == 'center', true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dit_milan_catalog.curated_schema.inventory SET ROW FILTER dit_milan_catalog.curated_schema.rls_function ON (area);

# COMMAND ----------

display(spark.read.table('dit_milan_catalog.curated_schema.inventory'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION dit_milan_catalog.curated_schema.cls_function (city STRING)
# MAGIC RETURN IF(is_account_group_member('dit_group'), hash(city), city);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dit_milan_catalog.curated_schema.inventory ALTER COLUMN city SET MASK dit_milan_catalog.curated_schema.cls_function;

# COMMAND ----------

display(spark.read.table('dit_milan_catalog.curated_schema.inventory'))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dit_milan_catalog.curated_schema.inventory DROP ROW FILTER

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dit_milan_catalog.curated_schema.inventory ALTER COLUMN city DROP MASK;
