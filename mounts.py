# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### accessing BLOB via access key

# COMMAND ----------

dbutils.fs.mount(
source= "wasbs://blobcontainer@blobstorageshellaccount.blob.core.windows.net",
mount_point= "/mnt/blobcontainer",
extra_configs={"fs.azure.account.key.blobstorageshellaccount.blob.core.windows.net":dbutils.secrets.get(scope = "adb-scope", key = "akv-blobstorage")}
)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

df1 = spark.read.csv("/mnt/blobcontainer/",header=True)

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = spark.read.csv("/FileStore/Tables/dept.csv",header=True)
df2.write.format("csv").save("/mnt/blobcontainer/dept.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### accessing GEN2 via service principle

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "1148faaf-98c7-4e7e-974b-d34544b79903",
"fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adb-scope",key="akv-app"),
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/551f957b-422a-4abd-b586-d551d6dd120a/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://adlsgen2container@datalakegen2shell.dfs.core.windows.net/",
  mount_point = "/mnt/adlsgen2container/",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/adlsgen2container/target/

# COMMAND ----------

dbutils.fs.unmount("/mnt/adlsgen2container/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### accessing GEN2 via access key

# COMMAND ----------

dbutils.fs.mount(
source= "wasbs://adlsgen2container@datalakegen2shell.blob.core.windows.net",
mount_point= "/mnt/adlsgen2container/",
extra_configs={"fs.azure.account.key.datalakegen2shell.blob.core.windows.net":dbutils.secrets.get(scope = "adb-scope", key = "akv-adlsgen2")}
)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


