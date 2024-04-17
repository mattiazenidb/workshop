# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

spark.sql(f"CREATE CATALOG {user_name}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA {user_name}.initial_schema")
