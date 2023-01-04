# Databricks notebook source
# MAGIC %md
# MAGIC # Directly Access ADLS Gen2 From Databricks
# MAGIC * Topic: How to Directly Access Containers and Directories of ADLS from Databricks?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # There are "Three Ways" to "Directly Access" the "Resources" Present in ADLS Gen2 from "Databricks" -
# MAGIC * A. Using "Access Key" of "Azure Data Lake Storage Gen2"
# MAGIC * B. Using "Shared Access Signature" Token of "Azure Data Lake Storage Gen2"
# MAGIC * C. Using "Azure Active Directory Service Principal"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a "Secret" in "Azure Key Vault" to Store the "Access Key" of "Azure Data Lake Storage Gen2"
# MAGIC * Step 1: Open the Azure Data Lake Storage Gen2 Resource "adlsoindrila2022march". Open the "Access keys" page by clicking on the "Access keys" menu under "Security + networking" category.
# MAGIC * Step 2: Copy the Value of one of the "Access Keys", e.g. "key1".
# MAGIC * Step 3: Use the copied Value of the Access Key "key1" to create the Secret "kv-secret-for-adlsoindrila2022march-access-key-1" in the Azure Key Vault Resource "KV-Oindrila-2022-March".

# COMMAND ----------

# DBTITLE 1,"Directly Access" the "Resources" of "ADLS Gen2" Using "Access Key" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "raw"
directoryPath = "input-csv-files/2022/08/04"
fileName = "Students_2.csv"

# Fetch the Value of the "Secret" Using "Databricks-Backed Secret Scope"
#kvSecretForAccessKey = dbutils.secrets.get("adb-backed-scope-july-2022", "secret-for-adlsoindrila2022march-access-key-1")
# Fetch the Value of the "Secret" Using "Azure Key Vault-Backed Secret Scope"
kvSecretForAccessKey = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-adlsoindrila2022march-access-key-1")

spark.conf.set(f"fs.azure.account.key.{storageAccountName}.dfs.core.windows.net", kvSecretForAccessKey)

# Read the CSV File "Students_2.csv", Present inside the Directory "input-csv-files/2022/08/04/" of the Container "raw"
filePath = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/{directoryPath}/{fileName}"

df = spark.read\
          .option("header", "true")\
          .csv(filePath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a "Secret" in "Azure Key Vault" to Store the "Shared Access Signature" Token of "Azure Data Lake Storage Gen2"
# MAGIC * Step 1: Open the Azure Data Lake Storage Gen2 Resource "adlsoindrila2022march". Open the "Access keys" page by clicking on the "Shared access signature" menu under "Security + networking" category.
# MAGIC * Step 2: Create a SAS Token and Copy the Value of "Blob SAS URL".
# MAGIC * Step 3: Use the copied Value of the "Blob SAS URL" to create the Secret "kv-secret-for-adlsoindrila2022march" in the Azure Key Vault Resource "KV-Oindrila-2022-March".

# COMMAND ----------

# DBTITLE 1,"Directly Access" the "Resources" of "ADLS Gen2" Using "Shared Access Signature Token" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "raw"
directoryPath = "input-csv-files/2022/08/04"
fileName = "Students_2.csv"

# Fetch the Value of the "Secret" Using "Databricks-Backed Secret Scope"
#kvSecretForSasKey = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-adlsoindrila2022march-sas-token")
# Fetch the Value of the "Secret" Using "Azure Key Vault-Backed Secret Scope"
kvSecretForSasKey = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-adlsoindrila2022march-sas-token")

spark.conf.set(f"fs.azure.account.auth.type.{storageAccountName}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageAccountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageAccountName}.dfs.core.windows.net", kvSecretForSasKey)

# Read the CSV File "Students_2.csv", Present inside the Directory "input-csv-files/2022/08/04/" of the Container "raw"
filePath = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/{directoryPath}/{fileName}"

df = spark.read\
          .option("header", "true")\
          .csv(filePath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Configure Access" to "Azure Data Lake Storage Gen 2" with an "Azure Active Directory Service Principal"
# MAGIC * Registering an "Azure Active Directory Service Principal" with "Azure Active Directory" (Azure AD) creates a "Service Principal" that can be used to provide "Access" to "Azure Data Lake Storage Gen 2". Then it is possible to "Configure Access" to these "Service Principals" using "Credentials" stored with "Secrets".
# MAGIC * Databricks recommends using "Azure Active Directory Service Principals" that is "Scoped" to "Clusters".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Register" an "Azure Active Directory Service Principal"
# MAGIC * To "Register" an "Azure Active Directory Service Principal" and "Assign" the appropriate "Permissions" on that "Azure Active Directory Service Principal" to access "Azure Data Lake Storage Gen2" resources, perform the following steps -
# MAGIC * Step 1: In the "Azure portal", go to the "Azure Active Directory" Service.
# MAGIC * Step 2: In the Left Menu, under "Manage" Category, click on the "App Registrations" Link.
# MAGIC * Step 3: Click on the "+ New registration" Link. Provide a Name for the "Azure Active Directory Service Principal" (e.g. "azure-ad-app-for-adlsoindrila2022march") in the Textbox "Name" and click on the "Register" Button.
# MAGIC * Step 4: The Page of the Created Azure Active Directory Service Principal opens. In the Left Menu, under "Manage" Category, Click on the "Certificates & secrets" Link.
# MAGIC * Step 5: Click on the "+ New client secret" Link.
# MAGIC * Step 6: Add a "Description" in the Textbox "Description" (e.g. "Creating Secret for the ADLS Resource adlsoindrila2022march") for the "Secret" and Click on the "Add" Button.
# MAGIC * Step 7: "Copy" and "Save" the "Value" for the "New Secret". This "Value" is actually the "Password" for the Created "Azure Active Directory Service Principal".
# MAGIC * Step 7.1: Create a "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key" for "Storing" the Value of the "New Secret", which is actually the "Password" for the Created "Azure Active Directory Service Principal"..
# MAGIC * Step 8: From the "Overview" Page of the Created "Azure Active Directory Service Principal", i.e., "azure-ad-app-for-adlsoindrila2022march", "Copy" and "Save" the Values for "Application (client) ID" and "Directory (tenant) ID". Databricks recommends "Storing" these "Credentials" using "Secrets".
# MAGIC * Step 8.1: Create a "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id" for "Storing" the "Application (client) ID".
# MAGIC * Step 8.2: Create another "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id" for "Storing" the "Directory (tenant) ID".

# COMMAND ----------

# MAGIC %md
# MAGIC # Assign "Roles" to the "Azure Active Directory Service Principal"
# MAGIC * It is possible to "Control" the "Access" to "Azure Data Lake Storage Gen2" resources by "Assigning" the proper "Roles" to an "Azure Active Directory Service Principal" that is "Associated" with the "Azure Data Lake Storage Gen2".
# MAGIC * Step 1: From the "Azure Portal", Open the "Azure Data Lake Storage Gen2" Instance to use with the "Azure Active Directory Service Principal".
# MAGIC * Step 2: Click on the "Access Control (IAM)" Left Menu Link.
# MAGIC * Step 3: Click on the "+ Add" Link and Select the "Add role assignment" from the Dropdown Menu.
# MAGIC * Step 4: Select "Storage Blob Data Contributor" in the "Role" Tab.
# MAGIC * Step 5: In the "Members" Tab, Select the RadioButton "User, group, or service principal" for "Assign access to" and Click on the "+ Select members" Link beside "Members". In the opened Right Blade, type the "Name" of the "Azure Active Directory Service Principal", i.e., "azure-ad-app-for-adlsoindrila2022march". Once the Application is appeared, select it and Click on the "Select" Button.
# MAGIC * Step 6: Finally, Click on the "Review + assign" Button Twice.

# COMMAND ----------

# DBTITLE 1,"Directly Access" the "Resources" of "ADLS Gen2" Using "Azure Active Directory Service Principal" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "raw"
directoryPath = "input-csv-files/2022/08/04"
fileName = "Students_2.csv"

kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", kvSecretForClientId)
spark.conf.set("fs.azure.account.oauth2.client.secret", kvSecretForClientSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token")

# Read the CSV File "Students_2.csv", Present inside the Directory "input-csv-files/2022/08/04/" of the Container "raw"
filePath = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/{directoryPath}/{fileName}"

df = spark.read\
          .option("header", "true")\
          .csv(filePath)

display(df)
