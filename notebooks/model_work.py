# Databricks notebook source
# DBTITLE 1,Chargement du modele metier
import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/fe223513d81f4ff98eb93ebee0efb270/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# COMMAND ----------

df = spark.read.table("hive_metastore.train.raw_data")

# COMMAND ----------

# DBTITLE 1,Prediction
df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))

# COMMAND ----------

df_selected = df.select("N°DPE", "Etiquette_DPE")
path = "/mnt/mount-storage/validation.csv"
df_selected.coalesce(1).write.csv(path, mode="append", header=True)

# COMMAND ----------


