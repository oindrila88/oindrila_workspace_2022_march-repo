# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 3
# MAGIC * Topic: How to Create the "Azure Key Vault-Backed Secret Scope"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Secret Scopes
# MAGIC * A "Secret Scope" is a "Mechanism" through which "Azure Databricks" tries to "Access" the "Secrets" or "Passwords", which are stored in some "Secured Place". A Databricks Workspace is limited to a maximum of 100 Secret Scopes.
# MAGIC * There are two types of "Secret Scope" -
# MAGIC * A. Azure Key Vault-backed Secret Scope ("Secrets", created in an "Azure Key Vault" Instance, can be Acessed from "Azure Databricks").
# MAGIC * B. Databricks-backed Secret Scope ("Secrets" can be created inside an "Encrypted Database", managed by "Azure Databricks").
# MAGIC * "Secret Scope" Names are "Case-Sensitive".
# MAGIC * "Secret Scope" Names can "Consist" of "Alphanumeric Characters", "Dashes", "Underscores", and "Periods", and may "Not Exceed 128 Characters".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why Use Secret Scope?
# MAGIC * In the "Notebook", when trying to "Access" the "Passwords" or "Secrets" or "Credentials", insted of "Hardcoding" those, it is possible to "Access" the "Secrets" that has the "Values" of those "Passwords" or "Secrets" or "Credentials" stored in a "Secret Scope".

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks-Backed Secret Scope
# MAGIC * A Databricks-backed Secret Scope is stored in an Encrypted Database, owned and managed by Azure Databricks.
# MAGIC * A Databricks-backed Secret Scope can be created using the Databricks CLI.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Databricks-Backed Secret Scope Using Databricks CLI
# MAGIC * By Default, the "Databricks-Backed Secret Scopes" are "Created" with "MANAGE" Permission for the "User", who "Created" the "secret Scope".
# MAGIC * If the "Databricks Account" is "Not" of Type "Premium Plan", the "Default" Permission must be "Overriden" and the "MANAGE" Permission should be "Explicitly Granted" to "All" the "Users" of the "Databricks Account", when "Creating" the "Databricks-Backed Secret Scope".
# MAGIC * A. Open the Azure Key Databricks CLI and Run the Command "databricks secrets create-scope --scope 'scope-name'" with the "Default Setting".
# MAGIC * B. Open the Azure Key Databricks CLI and Run the Command "databricks secrets create-scope --scope 'scope-name' --initial-manage-principal users" to "Create" a "Databricks-Backed Secret Scope" with "MANAGE" Permission for "All" the "Users" of the "Databricks Account".
# MAGIC * Example - databricks secrets create-scope --scope adb-backed-scope-july-2022 --initial-manage-principal users

# COMMAND ----------

# MAGIC %md
# MAGIC # Secret
# MAGIC * A "Secret" is a "Key-Value" Pair that "Stores" the "Secret Material", with a "Key" Name that is "Unique" within a "Secret Scope".
# MAGIC * Each "Secret Scope" is "Limited" to "1000 Secrets".
# MAGIC * The "Maximum Allowed Size" of the "Secret Value" is "128 KB".
# MAGIC * "Secret" Names are "Case-Insensitive".

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Secret in a Databricks-Backed Secret Scope
# MAGIC * Open the Azure Key Databricks CLI and Run the Command "databricks secrets put --scope 'scope-name' --key 'key-name'" to "Create" a "Secret" in a "Databricks-Backed Secret Scope" using the "Databricks CLI".
# MAGIC * An "Windows Explorer Editor" Opens. Paste the "Secret Value", "Save" and "Exit" the "Editor". The "Input" is "Stripped" of the "Comments" and "Stored" with the Associated "Key" in the "Databricks-Backed Secret Scope".
# MAGIC * If a "Key" Already Exists, the "New Value" Overwrites the "Existing Value".
# MAGIC * Example - databricks secrets put --scope adb-backed-scope-july-2022 --key secret-for-adlsoindrila2022march-access-key-1

# COMMAND ----------

# MAGIC %md
# MAGIC # List the Secrets in a Secret Scope
# MAGIC * Open the Azure Key Databricks CLI and Run the Command "databricks secrets list --scope 'scope-name'" to Display the "List of Secrets" Present in a given "Secret Scope".
# MAGIC * The "Response" Displays the "Metadata Information" about the "Secret", such as the "Secret Key Name" and "Last Updated Timestamp" (in "Milliseconds" since "Epoch").
# MAGIC * Example - databricks secrets list --scope adb-backed-scope-july-2022

# COMMAND ----------

# MAGIC %md
# MAGIC # Read a Secret from a Secret Scope
# MAGIC * It is possible to "Create" the "Secrets" using the "REST API" or "CLI", but Only the "Secrets Utility", i.e., the "dbutils.secrets" must be used in a "Notebook" or "Job" to "Read" a "Secret".

# COMMAND ----------

# DBTITLE 1,Read the Secret "secret-for-adlsoindrila2022march-access-key-1"
secretValue = dbutils.secrets.get("adb-backed-scope-july-2022", "secret-for-adlsoindrila2022march-access-key-1")
print(secretValue)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete a Secret from a Databricks-Backed Secret Scope
# MAGIC * Open the Azure Key Databricks CLI and Run the Command "databricks secrets delete --scope 'scope-name' --key 'key-name'" to "Delete" a "Secret" from a "Databricks-Backed Secret Scope".
# MAGIC * Example - databricks secrets delete --scope adb-backed-scope-july-2022 --key secret-for-adlsoindrila2022march-access-key-1

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Key Vault-Backed Secret Scope
# MAGIC * To reference Secrets, stored in an Azure Key Vault, an Azure Key Vault-backed Secret Scope can be created. All of the Secrets in the corresponding Key Vault instance can be leveraged from that Azure Key Vault-backed Secret Scope.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create an Azure Key Vault-Backed Secret Scope Using Azure Portal
# MAGIC * Step 1: Verify that if the "User" has "Contributor" Permission on the "Azure Key Vault" instance that the "User" wants to use to back the "Azure Key Vault-backed Secret Scope".
# MAGIC * Step 2: Open the Secret Management Page "https://'databricks-instance'/#secrets/createScope".
# MAGIC * Step 3: Create a New "Azure Key Vault-backed Secret Scope".
# MAGIC * Step 3.1: Provide "kv-oindrila-2022-march-secret-scope" in the "Scope Name" Textbox.
# MAGIC * Step 3.2: Select "All Users" from the Dropdown "Manage Principal", since this Databricks Workspace has the “Azure Databricks Standard Plan”.
# MAGIC * The "Manage Principal" Dropdown is used to specify whether "All Users" or Only the "Creator" of the "Secret Scope" have the "MANAGE" Permission.
# MAGIC * To Selet the "Creator" Option in the "Manage Principal" Dropdown, the "Databricks Account" must have the "Premium Plan".
# MAGIC * Step 3.3: Open the Azure Key Vault Resource "KV-Oindrila-2022-March". Open the "Properties" page by clicking on the "Properties" menu under "Settings" category -
# MAGIC * Step 3.3.1: Copy the Value of "Vault URI" and provide in the Textbox "DNS Name" under "Azure Key Vault" section of the Secret Management Page.
# MAGIC * Step 3.3.2: Copy the Value of "Resource ID" and provide in the Textbox "Resource ID" under "Azure Key Vault" section of the Secret Management Page.
# MAGIC * Step 4: Click on the "Create" button.

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify if the Azure Key Vault-Backed Secret Scope is Created from Azure Databricks CLI
# MAGIC * Open the Azure Key Databricks CLI and Run the Command "databricks secrets list-scopes" to Verify that the Azure Key Vault-Backed Secret Scope "kv-oindrila-2022-march-secret-scope" was Created Successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify if the Azure Key Vault-Backed Secret Scope is Created from Azure Key Vault
# MAGIC * Step 1: Open the Azure Key Vault Resource "KV-Oindrila-2022-March". Open the "Access policies" page by clicking on the "Access policies" menu under "Settings" category -
# MAGIC * Step 1.1: In the "APPLICATION" section of the "Access policies" page -
# MAGIC * Verify if the "Azure Databricks" Resource is added with "Get" and "List" Permissions to "Manage" All the "Secrets".

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete an Azure Key Vault-Backed Secret Scope from Azure Databricks CLI
# MAGIC * An "Azure Key Vault-Backed Secret Scope" can "Only" be "Deleted" from the "Databricks CLI" as there is no "dbutils.secret" Command to "Delete" a "Secret Scope".
# MAGIC * Open the Azure Key Databricks CLI and Run the Command "databricks secrets delete-scope --scope 'scope-name'" to Verify that the Azure Key Vault-Backed Secret Scope "kv-oindrila-2022-march-secret-scope" was Created Successfully.
# MAGIC * Example - databricks secrets delete-scope --scope kv-oindrila-2022-march-secret-scope
