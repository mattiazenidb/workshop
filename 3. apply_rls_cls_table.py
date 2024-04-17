# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

display(spark.read.table(f"{user_name}.initial_schema.inventory"))

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE FUNCTION {user_name}.initial_schema.rls_function (area STRING) RETURN IF(is_account_group_member('poste_group'), true, area == 'center')")

# COMMAND ----------

spark.sql(f"ALTER TABLE {user_name}.initial_schema.inventory SET ROW FILTER {user_name}.initial_schema.rls_function ON (area)");

# COMMAND ----------

display(spark.read.table(f"{user_name}.initial_schema.inventory"))

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE FUNCTION {user_name}.initial_schema.cls_function (city STRING) RETURN IF(is_account_group_member('poste_group'), city, hash(city))")

# COMMAND ----------

spark.sql(f"ALTER TABLE {user_name}.initial_schema.inventory ALTER COLUMN city SET MASK {user_name}.initial_schema.cls_function")

# COMMAND ----------

display(spark.read.table(f"{user_name}.initial_schema.inventory"))

# COMMAND ----------

spark.sql(f"ALTER TABLE {user_name}.initial_schema.inventory DROP ROW FILTER")

# COMMAND ----------

spark.sql(f"ALTER TABLE {user_name}.initial_schema.inventory ALTER COLUMN city DROP MASK")
