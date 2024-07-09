# Databricks notebook source
print("bla")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "helo world"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3 

# COMMAND ----------

# MAGIC %run ./Init

# COMMAND ----------

print(first_name)

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/")
display(files)

# COMMAND ----------


