# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %run "/Workspace/Users/deepesh.matkati@mmc.com/Day 1/includes"

# COMMAND ----------



# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("deepesh.iot_data")

# COMMAND ----------

# Reading file using external path defined in "Include notebook"
df=spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------


