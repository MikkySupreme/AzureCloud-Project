# Databricks notebook source
df_train = spark.read.table("hive_metastore.train.raw_data")
display(df_train)

# COMMAND ----------

# DBTITLE 1,On voit le nombre de lignes par Etiquette DPE
df_train.groupBy('Etiquette_DPE').count().show()

# COMMAND ----------

# DBTITLE 1,On va prendre le plus petit nombre d'Etiquette DEP
LIMIT = 12470
sample_df = spark.createDataFrame([], df_train.schema)
etiquette_dpe_values = df_train.select("Etiquette_DPE").distinct().rdd.map(lambda r: r[0]).collect()
for etiquette_dpe in etiquette_dpe_values:
    sample_df = sample_df.union(
        df_train.where(df_train["Etiquette_DPE"] == etiquette_dpe).limit(LIMIT)
    )

# COMMAND ----------

sample_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Creation de la table sample
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sample;

# COMMAND ----------

# DBTITLE 1,Stockage dans la database sample
sample_df_name = 'sample'
sample_df.write.format("delta").saveAsTable(f"{sample_df_name}.raw_data")
