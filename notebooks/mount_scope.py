# Databricks notebook source
# DBTITLE 1,Create Mount Point
dbutils.fs.mount(
  source = "wasbs://container1@stockdata1.blob.core.windows.net",
  mount_point = "/mnt/mount-storage",
  extra_configs = {"fs.azure.account.key.stockdata1.blob.core.windows.net": 
                    dbutils.secrets.get(scope = "databricks-scope1", 
                                        key = "access-key-container1")})
