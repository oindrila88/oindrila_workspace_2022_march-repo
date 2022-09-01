# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 1
# MAGIC * Topic: How to Access Containers and Directories of ADLS from Databricks Using "Azure Key Vault-Backed Secret Scope" by "Mounting".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Mounting"?
# MAGIC * "Mounting" is nothing but "Attaching" an "External Storage" to a "Local File System" so that the "External Storage" can be "Accessed" in such a way a "Local File System" is "Accessed".
# MAGIC * Databricks enables "Users" to "Mount" the "Cloud Object Storage" to the "Databricks File System" (DBFS) to simplify data access patterns for "Users" that are unfamiliar with cloud concepts.
# MAGIC * "Mounted Data" does not work with "Unity Catalog", and Databricks recommends migrating away from using "Mounts" and managing data governance with Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC # There are "Three Ways" to "Mount" ADLS Gen2 to "Databricks File System" (DBFS) -
# MAGIC * A. Using "Access Key"
# MAGIC * B. Using "SAS Token"
# MAGIC * C. Using "Registered Active Directory Application"

# COMMAND ----------

# MAGIC %md
# MAGIC # How does Databricks Mount the "Cloud Object Storage"?
# MAGIC * Databricks "Mounts" create a link between a "Workspace" and "Cloud Object Storage", which enables you to interact with "Cloud Object Storage" using familiar "File Paths" relative to the "Databricks File System" (DBFS). "Mounts" work by creating a local "Alias" under the "/mnt" Directory that stores the following information:
# MAGIC * A. Location of the "Cloud Object Storage".
# MAGIC * B. Driver specifications to connect to the "Storage Account" or "Container".
# MAGIC * C. "Security credentials" required to access the "Data".

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a "Secret" in "Azure Key Vault" to Store the "Access Key" of "Azure Data Lake Storage Gen2"
# MAGIC * Step 1: Open the Azure Data Lake Storage Gen2 Resource "adlsoindrila2022march". Open the "Access keys" page by clicking on the "Access keys" menu under "Security + networking" category.
# MAGIC * Step 2: Copy the Value of one of the "Access Keys", e.g. "key1".
# MAGIC * Step 3: Use the copied Value of the Access Key "key1" to create the Secret "kv-secret-for-adlsoindrila2022march-access-key-1" in the Azure Key Vault Resource "KV-Oindrila-2022-March".

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Access Key" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "databricks-training-container"
kvSecretForAccessKey = dbutils.secrets.get("adb-backed-scope-july-2022", "secret-for-adlsoindrila2022march-access-key-1")

accessKeyMountPoint = "/mnt/with-access-key/"

config = {
    "fs.azure.account.key.adlsoindrila2022march.dfs.core.windows.net": kvSecretForAccessKey
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == accessKeyMountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{accessKeyMountPoint}' Using Access Key Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'accessKeyMountPoint' Using Access Key")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net",
         mount_point = accessKeyMountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Access the "Mounted Path"
dbutils.fs.ls("/mnt/access-key/adlsoindrila2022march")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a "Secret" in "Azure Key Vault" to Store the "Shared Access Signature" Token of "Azure Data Lake Storage Gen2"
# MAGIC * Step 1: Open the Azure Data Lake Storage Gen2 Resource "adlsoindrila2022march". Open the "Access keys" page by clicking on the "Shared access signature" menu under "Security + networking" category.
# MAGIC * Step 2: Create a SAS Token and Copy the Value of "Blob SAS URL".
# MAGIC * Step 3: Use the copied Value of the "Blob SAS URL" to create the Secret "kv-secret-for-adlsoindrila2022march" in the Azure Key Vault Resource "KV-Oindrila-2022-March".

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Shared Access Signature Token" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "databricks-training-container"
kvSecretForSasKey = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-adlsoindrila2022march")

sasKeyMountPoint = "/mnt/with-sas-key/"

config = {
    "fs.azure.account.auth.type.adlsoindrila2022march.dfs.core.windows.net": "SAS",
    "fs.azure.sas.token.provider.type.adlsoindrila2022march.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
    "fs.azure.sas.fixed.token.adlsoindrila2022march.dfs.core.windows.net": kvSecretForSasKey
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == sasKeyMountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{sasKeyMountPoint}' Using SAS Key Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'sasKeyMountPoint' Using SAS Key")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net",
         mount_point = sasKeyMountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Access the "Mounted Path"
dbutils.fs.ls("/mnt/shared-access-signature/adlsoindrila2022march")

# COMMAND ----------

# MAGIC %md
# MAGIC # "Configure Access" to "Azure Data Lake Storage Gen 2" with an "Azure Active Directory Service Principal"
# MAGIC * Registering an "Application" with "Azure Active Directory" (Azure AD) creates a "Service Principal" that can be used to provide "Access" to "Azure Data Lake Storage Gen 2". Then it is possible to "Configure Access" to these "Service Principals" using "Credentials" stored with "Secrets".
# MAGIC * Databricks recommends using "Azure Active Directory Service Principals" that is "Scoped" to "Clusters".

# COMMAND ----------

# MAGIC %md
# MAGIC # Register an Azure Active Directory Application
# MAGIC * To "Register" an "Azure AD Application" and "Assign" the appropriate "Permissions" on that "Azure AD Application", which will create a "Service Principal" to access "Azure Data Lake Storage Gen2" resources, perform the following steps -
# MAGIC * Step 1: In the "Azure portal", go to the "Azure Active Directory" Service.
# MAGIC * Step 2: In the Left Menu, under "Manage" Category, click on the "App Registrations" Link.
# MAGIC * Step 3: Click on the "+ New registration" Link. Provide a Name for the "Application" (e.g. "azure-ad-app-for-adlsoindrila2022march") in the Textbox "Name" and click on the "Register" Button.
# MAGIC * Step 4: The Page of the Created Azure AD Application opens. In the Left Menu, under "Manage" Category, Click on the "Certificates & secrets" Link.
# MAGIC * Step 5: Click on the "+ New client secret" Link.
# MAGIC * Step 6: Add a "Description" in the Textbox "Description" (e.g. "Creating Secret for the ADLS Resource adlsoindrila2022march") for the "Secret" and Click on the "Add" Button.
# MAGIC * Step 7: "Copy" and "Save" the "Value" for the "New Secret".
# MAGIC * Step 7.1: Create a "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key" for "Storing" the Value of the "New Secret".
# MAGIC * Step 8: From the "Overview" Page of the Created Azure AD Application "azure-ad-app-for-adlsoindrila2022march", "Copy" and "Save" the Values for "Application (client) ID" and "Directory (tenant) ID". Databricks recommends "Storing" these "Credentials" using "Secrets".
# MAGIC * Step 8.1: Create a "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id" for "Storing" the "Application (client) ID".
# MAGIC * Step 8.2: Create another "Secret" in the Azure Key Vault "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id" for "Storing" the "Directory (tenant) ID".

# COMMAND ----------

# MAGIC %md
# MAGIC # Assign "Roles" to the Registered Azure Active Directory Application
# MAGIC * It is possible to "Control" the "Access" to "Azure Data Lake Storage Gen2" resources by "Assigning" the proper "Roles" to a "Registered Azure Active Directory Application" that is "Associated" with the "Azure Data Lake Storage Gen2".
# MAGIC * Step 1: From the "Azure Portal", Open the "Azure Data Lake Storage Gen2" Instance to use with the "Registered Azure Active Directory Application".
# MAGIC * Step 2: Click on the "Access Control (IAM)" Left Menu Link.
# MAGIC * Step 3: Click on the "+ Add" Link and Select the "Add role assignment" from the Dropdown Menu.
# MAGIC * Step 4: Select "Storage Blob Data Contributor" in the "Role" Tab.
# MAGIC * Step 5: In the "Members" Tab, Select the RadioButton "User, group, or service principal" for "Assign access to" and Click on the "+ Select members" Link beside "Members". In the opened Right Blade, type the "Name" of the "Registered Azure Active Directory Application", i.e., "azure-ad-app-for-adlsoindrila2022march". Once the Application is appeared, select it and Click on the "Select" Button.
# MAGIC * Step 6: Finally, Click on the "Review + assign" Button Twice.

# COMMAND ----------

# DBTITLE 1,"Mount" Using "Registered Azure Active Directory Application" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "databricks-training-container"
kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

aadAppMountPoint = "/mnt/with-aad-app/"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": kvSecretForClientId,
    "fs.azure.account.oauth2.client.secret": kvSecretForClientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token"
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == aadAppMountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{aadAppMountPoint}' Using AAD App Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'aadAppMountPoint' Using AAD App")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net",
         mount_point = aadAppMountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Access the "Mounted Path"
dbutils.fs.ls("/mnt/with-aad-app/")

# COMMAND ----------

# DBTITLE 1,"Mount" Another "Container" of "ADLS Gen2" Using "Registered Azure Active Directory Application" Via "Azure Key Vault-Backed Secret Scope"
storageAccountName = "adlsoindrila2022march"
containerName = "databrcks-training-container"
kvSecretForClientId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-client-id")
kvSecretForClientSecret = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-secret-key")
kvSecretForTenantId = dbutils.secrets.get("kv-oindrila-2022-march-secret-scope", "kv-secret-for-azure-ad-app-for-adlsoindrila2022march-tenant-id")

aadAppMountPoint = "/mnt/with-aad-app/databrcks-training-container"

config = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": kvSecretForClientId,
    "fs.azure.account.oauth2.client.secret": kvSecretForClientSecret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{kvSecretForTenantId}/oauth2/token"
}

# Determine If the "Mount Point" is Not Already Mounted
for mount in dbutils.fs.mounts():
    ifMountPointExists = (mount.mountPoint == aadAppMountPoint)
    if ifMountPointExists:
        print(f"Mount Point '{aadAppMountPoint}' Using AAD App Already Exists")
        break
        
# Create the "Mount Point", If It Does Not Exists
if not ifMountPointExists:
    print(f"Creating the Mount Point 'aadAppMountPoint' Using AAD App")
    dbutils.fs.mount(
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net",
         mount_point = aadAppMountPoint,
         extra_configs = config
    )

# COMMAND ----------

# DBTITLE 1,Copy "Folders" and "Files" "Recursively" From "One Directory" to "Another Directory" Using "Mounted Path"
# "Copy" All the "Folders" and "Files" "Recursively" from the Container "databrcks-training-container" to Another Container "databricks-training-container".
dbutils.fs.cp('/mnt/with-aad-app/databrcks-training-container/', "/mnt/with-aad-app/databricks-training-folder/", True)

# COMMAND ----------

# DBTITLE 1,View How Many "External Storages" are "Mounted" in the Databricks Environment
dbutils.fs.mounts()

# COMMAND ----------

# DBTITLE 1,"Unmount" a "Mounted" Path
dbutils.fs.unmount("/mnt/with-sas-key/")
