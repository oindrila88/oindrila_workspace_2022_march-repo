# Databricks notebook source
# MAGIC %md
# MAGIC # Storing Data of a Table from "Azure SQL Server" To "ADLS" via "Azure Databricks"
# MAGIC * Topic: How To Store Data of a Table from "Azure SQL Server" To a "Container" of an "ADLS" via "Azure Databricks".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC * "Azure Databricks" supports connecting to "External Databases" using "JDBC".
# MAGIC * "Databricks" recommends using "Secrets" to store the "Database Credentials".

# COMMAND ----------

# DBTITLE 1,Get the Values of the Required "Secret" Using "Key-Vault Backed Secret Scope"
sqlServerName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-sqlservername")
sqlDbName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-sqldbname")
userName = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-username")
password = dbutils.secrets.get(scope = "kv-oindrila-2022-march-secret-scope", key = "kv-sqlserveroindrila2022march-pwd")

# COMMAND ----------

# MAGIC %md
# MAGIC * A number of settings must be configured to read data using JDBC. Each "Database" uses a different format for the "<jdbc-url>".

# COMMAND ----------

# DBTITLE 1,Create a DataFrame from the Table to Store
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

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Azure Active Directory Service Principal" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "sqlserverfiles-container"
kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

mountPoint = "/mnt/sqlserverfiles/"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": kvSecretForClientId,
    "fs.azure.account.oauth2.client.secret": kvSecretForClientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token"
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == mountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{mountPoint}' Using AAD App Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'aadAppMountPoint' Using AAD App")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
         mount_point = mountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Save the Contents of the "Employee" SQL Server Table as "CSV" in ADLS Using Mounted Path
from datetime import datetime

currentYear = datetime.now().year
currentMonth = datetime.now().month
currentDay = datetime.now().day
currentHour = datetime.now().hour
currentMinute = datetime.now().minute
currentSecond = datetime.now().second

tempFilePath = f"{mountPoint}/tempFolder/"
finalFilePathWithFileName = f"{mountPoint}/EmployeeTable/{currentYear}/{currentMonth}/{currentDay}/Employee_{currentHour}_{currentMinute}_{currentSecond}.csv"

dfEmployeeTable.coalesce(1)\
               .write\
               .format("csv")\
               .mode("Append")\
               .options(\
                            path = tempFilePath,\
                            header = "true"\
                       )\
               .save()

allTempFiles = dbutils.fs.ls(tempFilePath)
csvFileName = ""
for tempFile in allTempFiles:
    if tempFile.name.endswith(".csv"):
        csvFileName = tempFile.name

print("File Name : " + csvFileName)

dbutils.fs.cp(tempFilePath + csvFileName, finalFilePathWithFileName)

dbutils.fs.rm(tempFilePath, True)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame from the Stored "CSV File"
dfEmployee = spark.read.format("csv").option("header", "true").load(finalFilePathWithFileName)
display(dfEmployee)

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Azure Active Directory Service Principal" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "delta-table-container"
kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

mountPoint = "/mnt/deltatables/"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": kvSecretForClientId,
    "fs.azure.account.oauth2.client.secret": kvSecretForClientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token"
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == mountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{mountPoint}' Using AAD App Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'aadAppMountPoint' Using AAD App")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
         mount_point = mountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Create a Database
deltaTableSchemaName = "demoSchema"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {deltaTableSchemaName}")

# COMMAND ----------

# DBTITLE 1,Create an Unmanaged Delta Table
deltaTableName = "Employee"
deltaTablePath = "/mnt/deltatables/Employee/"

spark.sql(f"CREATE TABLE IF NOT EXISTS {deltaTableSchemaName}.{deltaTableName} USING DELTA LOCATION '{deltaTablePath}'")

# COMMAND ----------

# DBTITLE 1,Load the Content of the DataFrame "dfEmployee" into the Delta Table "demoSchema.Employee"
dfEmployee.write.format("delta")\
          .option("mergeSchema", "true")\
          .mode("append")\
          .saveAsTable(f"{deltaTableSchemaName}.{deltaTableName}")

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "demoSchema.Employee"
# MAGIC %sql
# MAGIC SELECT * FROM demoSchema.Employee;

# COMMAND ----------

# DBTITLE 1,Again Create Source DataFrame, i.e., "dfEmployeeTable" With Fewer Columns
from datetime import datetime

currentYear = datetime.now().year
currentMonth = datetime.now().month
currentDay = datetime.now().day
currentHour = datetime.now().hour
currentMinute = datetime.now().minute
currentSecond = datetime.now().second

mountPoint = "/mnt/sqlserverfiles/"
tempFilePath = f"{mountPoint}/tempFolder/"
finalFilePathWithFileName = f"{mountPoint}EmployeeTable/{currentYear}/{currentMonth}/{currentDay}/Employee_{currentHour}_{currentMinute}_{currentSecond}.csv"
print("Final File Path : " + finalFilePathWithFileName)

#listEmployee = [(7, "New York"), (8, "Tokyo")]
listEmployee = [(9, "Berlin"), (10, "Nara")]
listEmployeeSchema = ["Id", "Address"]

dfEmployeeTable = spark.createDataFrame(listEmployee, listEmployeeSchema)
display(dfEmployeeTable)

dfEmployeeTable.coalesce(1)\
               .write\
               .format("csv")\
               .mode("Append")\
               .options(\
                            path = tempFilePath,\
                            header = "true"\
                       )\
               .save()

allTempFiles = dbutils.fs.ls(tempFilePath)
csvFileName = ""
for tempFile in allTempFiles:
    if tempFile.name.endswith(".csv"):
        csvFileName = tempFile.name

print("File Name : " + csvFileName)

dbutils.fs.cp(tempFilePath + csvFileName, finalFilePathWithFileName)

dbutils.fs.rm(tempFilePath, True)

# COMMAND ----------

# DBTITLE 1,Again, Create a DataFrame from the Stored "CSV File"
dfEmployee = spark.read.format("csv").option("header", "true").load(finalFilePathWithFileName)
display(dfEmployee)

# COMMAND ----------

# DBTITLE 1,Again, Load the Content of the DataFrame "dfEmployee" into the Delta Table "demoSchema.Employee"
dfEmployee.write.format("delta")\
          .option("mergeSchema", "true")\
          .mode("append")\
          .saveAsTable(f"{deltaTableSchemaName}.{deltaTableName}")

# COMMAND ----------

# DBTITLE 1,Check the Data Present in the Delta Table "demoSchema.Employee"
# MAGIC %sql
# MAGIC SELECT * FROM demoSchema.Employee;

# COMMAND ----------

# MAGIC %md
# MAGIC Archival Process

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Azure Active Directory Service Principal" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "archive-container"
kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

mountPoint = "/mnt/archivecontainer/"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": kvSecretForClientId,
    "fs.azure.account.oauth2.client.secret": kvSecretForClientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token"
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == mountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{mountPoint}' Using AAD App Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'aadAppMountPoint' Using AAD App")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
         mount_point = mountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Move All the Files, Which are More Than 3 Days Old to the "Archive Directory"
import os
import pathlib
import datetime

pathToCheckForArchival = "/dbfs/mnt/sqlserverfiles/EmployeeTable/"

# Verify If the Path Exists
if not os.path.exists(pathToCheckForArchival):
    raise(f"Path '{pathToCheckForArchival}' is Invalid")
# Verify If the Path is a "Directory"
if os.path.isfile(pathToCheckForArchival):
    raise(f"Path '{pathToCheckForArchival}' is a File")

#Get All the "Directories", "Sub-Directories" and "Files" Present Inside the "Provided Path" in a "List"
listOfAllDirAndFiles = list(pathlib.Path(pathToCheckForArchival).rglob("*"))

#"Loop" Through the "List" to "Fetch" Only the "Files"
for eachEntity in listOfAllDirAndFiles:
    if os.path.isfile(eachEntity):
        #Get the "File Creation Time" of "Each File" Using "getctime" Method, Which Provides Result in "Timestamp Seconds". Using "fromtimestamp" Method of "datetime" Module, the "Timestamp Seconds" Needs to be "Converted" into "Proper Datetime Format"
        fileCreationDateTime = datetime.datetime.fromtimestamp(os.path.getctime(eachEntity))
        print("File Creation Time : " + str(fileCreationDateTime))

        currentDateTime = datetime.datetime.now()
        print("Current Time : " + str(currentDateTime))

        #Find "How Many Days" Before the "File" was "Created"
        fileAgeInDays = (currentDateTime - fileCreationDateTime).days

        #"Move" the "File" to "Archive Directory" Which are "Older Than 2 Days"
        if fileAgeInDays >= 0:
            print(f"The File '{eachEntity}' was Created '{fileAgeInDays} Days' Back. Hence, Moving the File to 'Archive'")

            #Get the "File Name" from the "File Path"
            filePath, fileName = os.path.split(eachEntity)
            print(fileName)

            #"Move" the "File" to "Archive Directory"
            eachEntity = str(eachEntity)
            dbutils.fs.mv(eachEntity[eachEntity.find('dbfs') + len('dbfs'):], f"/mnt/archivecontainer/EmployeeTable/{fileName}", True)

# COMMAND ----------

# DBTITLE 1,"Delete" All the "Empty Directories"
import os
import pathlib

pathToCheckForArchival = "/dbfs/mnt/sqlserverfiles/EmployeeTable/"

# Verify If the Path Exists
if not os.path.exists(pathToCheckForArchival):
    raise(f"Path '{pathToCheckForArchival}' is Invalid")
# Verify If the Path is a "Directory"
if os.path.isfile(pathToCheckForArchival):
    raise(f"Path '{pathToCheckForArchival}' is a File")

#Get All the "Directories", "Sub-Directories" and "Files" Present Inside the "Provided Path" in a "List"
listOfAllDirAndFiles = list(pathlib.Path(pathToCheckForArchival).rglob("*"))

#"Loop" Through the "List" to "Fetch" Only the "Directories"
for eachEntity in listOfAllDirAndFiles:
    if not os.path.isfile(eachEntity):
        #Checking if the "Directory" is "Empty" or "Not"
        if not os.listdir(eachEntity):
            print(f"The Directory '{eachEntity}' is Empty. Removing the Empty Directory")
            eachEntity = str(eachEntity)
            dbutils.fs.rm(eachEntity[eachEntity.find('dbfs') + len('dbfs'):])
