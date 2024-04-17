# Databricks notebook source
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

display(spark.read.table('federated_synapse_catalog.dbo.users').limit(1))

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE VIEW {user_name}.initial_schema.synapse_curated_view AS
            SELECT
                object_id,
                CASE 
                WHEN is_account_group_member('dit_demo') THEN column_name
                ELSE 'REDACTED'
                END AS column_name,
                column_id,
                column_type,
                column_ordinal,
                is_computed,
                masking_function
            FROM federated_synapse_catalog.dbo.users
          """)

# COMMAND ----------

display(spark.read.table(f"{user_name}.initial_schema.synapse_curated_view").limit(1))
