# Databricks notebook source
import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/1f27173e39b54bf48ad3ceb166812e19/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double', env_manager="conda")
# Predict on a Spark DataFrame.
df = spark.read.table("hive_metastore.val.raw_data")
df.withColumn('Etiquette_DPE', loaded_model(struct(*map(col, df.columns))))

# COMMAND ----------

df_selected = df.select("N_DPE", "Etiquette_DPE")
path = "/mnt/mount-storage/validation.csv"
df_selected.coalesce(1).write.csv(path, mode="append", header=True)

# COMMAND ----------


