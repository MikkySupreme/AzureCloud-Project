# Databricks notebook source
df_train = spark.read.table("hive_metastore.train.raw_data")
display(df_train)

# COMMAND ----------

df_train.groupBy('Etiquette_DPE').count().show()

# COMMAND ----------


