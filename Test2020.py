# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df = spark.read.format("csv").option("header",true).load("/FileStore/Tables/dept.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("csv").mode("overwrite").saveAsTable("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dept

# COMMAND ----------


