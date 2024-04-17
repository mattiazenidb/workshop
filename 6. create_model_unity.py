# Databricks notebook source
# MAGIC %pip install --upgrade "mlflow-skinny[databricks]>=2.11.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split

# COMMAND ----------

import mlflow 
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

mlflow.set_registry_uri("databricks-uc")

dataset = mlflow.data.load_delta(table_name=f"{user_name}.initial_schema.warehouse_stocks_curated_features", version="0")
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
catalog = user_name
schema = "initial_schema"
model_name = "forecasting_model"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/456bdfff30b04788a19e363d4e0a6430/rf_model", f"{catalog}.{schema}.{model_name}")
