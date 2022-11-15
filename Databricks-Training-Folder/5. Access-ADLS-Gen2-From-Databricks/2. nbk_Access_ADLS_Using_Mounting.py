# Databricks notebook source
# MAGIC %md
# MAGIC # Mount ADLS Gen2 Into Databricks
# MAGIC * Topic: How to Access Containers and Directories of ADLS from Databricks Using "Azure Key Vault-Backed Secret Scope" by "Mounting"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Mounting"?
# MAGIC * "Mounting" is nothing but "Attaching" an "External Storage" to a "Local File System" so that the "External Storage" can be "Accessed" in such a way a "Local File System" is "Accessed".
# MAGIC * Databricks enables "Users" to "Mount" the "Cloud Object Storage" to the "Databricks File System" (DBFS) to simplify data access patterns for "Users" that are unfamiliar with cloud concepts.
# MAGIC * "Mounted Data" does not work with "Unity Catalog", and Databricks recommends migrating away from using "Mounts" and managing data governance with Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC # There is Only "One Way" to "Mount" ADLS Gen2 to "Databricks File System" (DBFS) -
# MAGIC * Using "Registered Azure Active Directory Service Principal"

# COMMAND ----------

# MAGIC %md
# MAGIC # How does Databricks Mount the "Cloud Object Storage"?
# MAGIC * Databricks "Mounts" create a link between a "Workspace" and "Cloud Object Storage", which enables you to interact with "Cloud Object Storage" using familiar "File Paths" relative to the "Databricks File System" (DBFS). "Mounts" work by creating a local "Alias" under the "/mnt" Directory that stores the following information:
# MAGIC * A. Location of the "Cloud Object Storage".
# MAGIC * B. Driver specifications to connect to the "Storage Account" or "Container".
# MAGIC * C. "Security credentials" required to access the "Data".

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

# DBTITLE 1,"Mount" Using "Azure Active Directory Service Principal" Via "Azure Key Vault-Backed Secret Scope"
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
         source = f"abfss://{containerName}@{storageAccountName}.dfs.core.windows.net/",
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
dbutils.fs.cp('/mnt/with-aad-app/dataabrcks-training-continer/', "/mnt/with-aad-app/databricks-training-folder/", True)

# COMMAND ----------

# DBTITLE 1,View How Many "External Storages" are "Mounted" in the Databricks Environment
dbutils.fs.mounts()

# COMMAND ----------

# DBTITLE 1,"Update" a "Mount Point"
# The "dbutils.fs.updateMount" Command "Updates" an "Existing Mount Point", instead of "Creating" a "New One", i.e., "Points" the "Existing Mount Point" to a "New Cloud Object Storage Location Path".
# The "dbutils.fs.updateMount" Command "Returns" an "Error" if the "Mount Point" is "Not Present".
oldMountPoint = "/mnt/with-aad-app/databricks-training-folder/"
mewMountPoint = "/mnt/with-aad-app/new-databricks-training-folder/"
dbutils.fs.updateMount()

# COMMAND ----------

# DBTITLE 1,"Refresh" the "Mount Points"
# The "dbutils.fs.refreshMounts()" Command "Forces" "All the Machines" in the "Cluster" to "Refresh" the "Mount Cache", ensuring that the "Mount Cache" receives the "Most Recent Information" about the "Mount Points".
dbutils.fs.refreshMounts()

# COMMAND ----------

# DBTITLE 1,"Unmount" a "Mounted" Path
# The "dbutils.fs.unmount()" Command "Deletes" a "DBFS Mount Point".
dbutils.fs.unmount("/mnt/with-sas-key/")
