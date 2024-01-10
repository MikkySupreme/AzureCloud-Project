# Databricks notebook source
from pyspark.sql.functions import col, avg
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

# DBTITLE 1,Dataviz des données Train
df_train = spark.read.table("hive_metastore.train.raw_data")
display(df_train)
df_train.drop('_c0')

# COMMAND ----------

# DBTITLE 1,Conversion de la colonne "Etiquette DPE" en valeurs numériques
df_pandas = df_train.toPandas()
data = df_pandas.drop('_c0', axis=1)
dpe_mapping = {'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5, 'F': 6, 'G': 7}
data['Etiquette_DPE_Num'] = data['Etiquette_DPE'].map(dpe_mapping)

# COMMAND ----------

# DBTITLE 1,Sélection des colonnes numériques
numerical_cols = data.select_dtypes(include=['number']).columns.tolist()

# COMMAND ----------

# DBTITLE 1,Calcul de la matrice de corrélation
correlation_matrix = data[numerical_cols].corr(method='spearman')

# COMMAND ----------

# DBTITLE 1,Graphe de la matrice de corrélation
plt.figure(figsize=(14, 12))  # Ajustez selon les besoins
heatmap = sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm', square=True)
plt.title('Matrice de corrélation')
plt.show()

# COMMAND ----------

# DBTITLE 1,Les nombres de lignes vides par colonne
empty_counts = {col: df_train.filter((df_train[col] == "") | df_train[col].isNull()).count() for col in df_train.columns}
plt.figure(figsize=(10, 6))
plt.bar(empty_counts.keys(), empty_counts.values())
plt.xlabel('Colonnes')
plt.ylabel('Nombre de lignes vides')
plt.title('Nombre de lignes vides par colonne')
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# DBTITLE 1,Ratio des Etiquettes DPE par rapport au nombre total de lignes
dpe_counts = df_train.groupBy("Etiquette_DPE").count()
dpe_counts_pandas = dpe_counts.toPandas()

# COMMAND ----------

plt.figure(figsize=(8, 8))
plt.pie(dpe_counts_pandas['count'], labels=dpe_counts_pandas['Etiquette_DPE'], autopct='%1.1f%%')
plt.title('Ratio des Etiquettes DPE')
plt.show()

# COMMAND ----------

count_by_year = df_train.groupBy('Annete_construction').count().orderBy(col('Annete_construction').asc())
count_by_year.show()

# COMMAND ----------

df_train.printSchema()

# COMMAND ----------

# DBTITLE 1,Moyenne de consommation des 5 usages pour chaque étiquette DPE
average_values = df_train.groupBy("Etiquette_DPE").agg(
    avg("Conso_5_usages_m2_et_finale").alias("Moyenne_Conso_5_usages_m2_et_finale"),
    avg("Conso_5_usages_et_finale").alias("Moyenne_Conso_5_usages_et_finale")
)

average_values.show()

# COMMAND ----------


