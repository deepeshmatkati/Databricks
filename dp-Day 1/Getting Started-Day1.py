# Databricks notebook source
# MAGIC %sql
# MAGIC create schema deepesh

# COMMAND ----------

# MAGIC %sql
# MAGIC use deepesh

# COMMAND ----------

data = ([(1,'a',30),(2,'b',34)])
schema = "id int, name string, age int"
df=spark.createDataFrame(data,schema)


# COMMAND ----------

df.show()

# COMMAND ----------

#https://docs.databricks.com/en/dbfs/index.html#what-is-the-databricks-file-system-dbfs

# COMMAND ----------

dbutilites
dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/processed/emp.csv","dbfs:/FileStore/tables/raw/")


# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/processed")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True, inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.saveAsTable("deepesh.emp_demp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deepesh.emp_demp where id=1

# COMMAND ----------


