# Databricks notebook source
# DBTITLE 1,Importation des librairies nécessaires
from pyspark.sql import function as F

# COMMAND ----------

# DBTITLE 1,Chargement des données de la table test
df = spark.table("test")

# COMMAND ----------


