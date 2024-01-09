# Databricks notebook source
# DBTITLE 1,Les imports 
import re

# COMMAND ----------

# DBTITLE 1,Chargement des CSV dans le dossier /mnt
df_train = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mount-storage/train.csv")

df_test = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mount-storage/test.csv")

df_val = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/mount-storage/train.csv")

# COMMAND ----------

df_test.printSchema()

# COMMAND ----------

# DBTITLE 1,Fonction nettoyage des noms de colonnes avec Regex
def clean_column_name(column_name):
    column_name = column_name.replace('²', '2')
    column_name = column_name.replace('é', 'et')
    return re.sub(r'[ ,;{}()\n\t=/°]', '_', column_name)

# COMMAND ----------

# DBTITLE 1,Nettoyage des noms de colonnes
clean_df_train = [clean_column_name(c) for c in df_train.columns]
clean_df_test = [clean_column_name(c) for c in df_test.columns]
clean_df_val = [clean_column_name(c) for c in df_val.columns]

# COMMAND ----------

df_train_cleaned = df_train.toDF(*clean_df_train)
df_test_cleaned = df_test.toDF(*clean_df_test)
df_val_cleaned = df_val.toDF(*clean_df_val)

# COMMAND ----------

df_test_cleaned.printSchema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS train;
# MAGIC CREATE DATABASE IF NOT EXISTS test;
# MAGIC CREATE DATABASE IF NOT EXISTS val;

# COMMAND ----------

# DBTITLE 1,Transformation en raw data
db_train = 'train'
df_train_cleaned.write.format("delta").saveAsTable(f"{db_train}.raw_data")

db_test = 'test'
df_test_cleaned.write.format("delta").saveAsTable(f"{db_test}.raw_data")

db_val = 'val'
df_val_cleaned.write.format("delta").saveAsTable(f"{db_val}.raw_data")
