# Databricks notebook source
# MAGIC %md
# MAGIC # Connecting From "Azure SQL Server" To "Azure Databricks"
# MAGIC * Topic: How To Connect From "Azure SQL Server" To "Azure Databricks".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC * "Azure Databricks" supports connecting to "External Databases" using "JDBC".
# MAGIC * "Databricks" recommends using "Secrets" to store the "Database Credentials".

# COMMAND ----------

sqlServerName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-sqlservername")
sqlDbName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-sqldbname")
userName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-username")
password = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-pwd")

# COMMAND ----------

# MAGIC %md
# MAGIC * A number of settings must be configured to read data using JDBC. Each "Database" uses a different format for the "<jdbc-url>".

# COMMAND ----------

tableName = "dbo.Employee"
jdbcUrl = f"jdbc:sqlserver://{sqlServerName}.database.windows.net:1433;database={sqlDbName};user={userName}@sqlserveroindrila2022march;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

dfEmployeeTable = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", tableName)
  .option("user", userName)
  .option("password", password)
  .load()
)

display(dfEmployeeTable)
