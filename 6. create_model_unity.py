# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]>=2.11.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

# COMMAND ----------

import mlflow 
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

mlflow.set_registry_uri("databricks-uc")

dataset = mlflow.data.load_delta(table_name="dit_milan_catalog.curated_schema.warehouse_stocks_curated_features", version="0")
pd_df = dataset.df.toPandas()
X = pd_df.drop("price", axis=1)
y = pd_df["price"]

with mlflow.start_run(run_name="Test run") as run:    
    rf = RandomForestRegressor(random_state=42)
    rf_model = rf.fit(X, y)

    predicted_qualities = rf_model.predict(X)

    signature = mlflow.models.infer_signature(model_input=X[:10], model_output=predicted_qualities[:10])
    
    mlflow.log_input(dataset, context="training")
    mlflow.sklearn.log_model(rf_model, "rf_model", signature=signature)

# COMMAND ----------

# DBTITLE 1,mlflow register model in catalog
catalog = "dit_milan_catalog"
schema = "curated_schema"
model_name = "forecasting_model"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/e66faf3bd6be46918120cfe65cc35293/rf_model", f"{catalog}.{schema}.{model_name}")
