# Databricks notebook source
# DBTITLE 1,Create Mount Point
dbutils.fs.mount(
  source = "wasbs://container1@stockdata1.blob.core.windows.net",
  mount_point = "/mnt/mount-storage",
  extra_configs = {"fs.azure.account.key.stockdata1.blob.core.windows.net": 
                    dbutils.secrets.get(scope = "databricks-scope1", 
                                        key = "access-key-container1")})

# COMMAND ----------

# DBTITLE 1,Chargement du csv
df = spark.read.option("header", "true").csv("/mnt/mount-storage/test.csv")

# COMMAND ----------

# DBTITLE 1,Renommage des colonnes
rename_mapping = {
    "N°DPE": "num_dpe",
    "Configuration_installation_chauffage_n°2": "config_chauffage_n2",
    "Facteur_couverture_solaire_saisi": "facteur_couv_solaire",
    "Surface_habitable_desservie_par_installation_ECS": "surf_hab_ecs",
    "Emission_GES_éclairage": "emission_ges_eclairage",
    "Cage_d'escalier": "cage_escalier",
    "Conso_5_usages_é_finale_énergie_n°2": "conso_usages_finale_energie_n2",
    "Type_générateur_froid": "type_generateur_froid",
    "Type_émetteur_installation_chauffage_n°2": "type_emetteur_chauffage_n2",
    "Surface_totale_capteurs_photovoltaïque": "surface_total_capteurs_pv",
    "Nom__commune_(Brut)": "nom_commune",
    "Conso_chauffage_dépensier_installation_chauffage_n°1": "conso_chauffage_n1",
    "Coût_chauffage_énergie_n°2": "cout_chauffage_energie_n2",
    "Emission_GES_chauffage_énergie_n°2": "emission_ges_chauffage_energie_n2",
    "Code_INSEE_(BAN)": "code_insee",
    "Type_énergie_n°3": "type_energie_n3",
    "Etiquette_GES": "etiquette_ges",
    "Type_générateur_n°1_installation_n°2": "type_generateur1_install_n2",
    "Code_postal_(brut)": "code_postal",
    "Description_générateur_chauffage_n°2_installation_n°2": "desc_generateur_chauffage2_install2",
    "Facteur_couverture_solaire": "facteur_couv_solaire_simple",
    "Année_construction": "annee_construction",
    "Classe_altitude": "classe_altitude",
    "Code_postal_(BAN)": "code_postal_insee",
    "Conso_5_usages/m²_é_finale": "conso_usages_m2_finale",
    "Conso_5_usages_é_finale": "conso_usages_finale",
    "Etiquette_DPE": "etiquette_dpe",
    "Hauteur_sous-plafond": "hauteur_plafond",
    "N°_département_(BAN)": "num_departement",
    "Qualité_isolation_enveloppe": "qualite_iso_enveloppe",
    "Qualité_isolation_menuiseries": "qualite_iso_menuiseries",
    "Qualité_isolation_murs": "qualite_iso_murs",
    "Qualité_isolation_plancher_bas": "qualite_iso_plancher_bas",
    "Qualité_isolation_plancher_haut_comble_aménagé": "qualite_iso_plancher_haut_comble_amenage",
    "Qualité_isolation_plancher_haut_comble_perdu": "qualite_iso_plancher_haut_comble_perdu",
    "Qualité_isolation_plancher_haut_toit_terrase": "qualite_iso_plancher_haut_toit_terr",
    "Surface_habitable_immeuble": "surf_hab_immeuble",
    "Surface_habitable_logement": "surf_hab_logement",
    "Type_bâtiment": "type_batiment"
}

for old_name, new_name in rename_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Creation de la database test dans le catalogue 
# MAGIC %sql
# MAGIC create database test
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ajout des données du dataframe dans la table test 
db_name = "test"
df.write.format("delta").saveAsTable(f"{db_name}.raw_data")
