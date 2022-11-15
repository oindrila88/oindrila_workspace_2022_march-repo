# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks CLI
# MAGIC * Topic: Introduction To "Databricks CLI".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why Databricks CLI is Used?
# MAGIC * The Databricks Command-Line Interface (Databricks CLI) is Nothing but a "Python Package" and the name of the Python Package is "databricks-cli". Once, the "Databricks CLI Python Package" is "Installed" on the "Local Machine", using the different "Utility Commands" it is possible to "Interact" or "Communicate" with the "Azure Databricks Workspace" from that "Local Machine".
# MAGIC * Python of Version 3.6 and above Should be Installed in the "Local Machine" for the "Databricks CLI Python Package" to be "Installed".
# MAGIC * Using the "Python Package Manager" Tool. i.e., "pip", it is possible to "Install" any "Python Package".
# MAGIC * To "Install" the Python Package is "databricks-cli" from the "Local Machine", use the Command "pip install databricks-cli" in the "Command Prompt".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to See What are the Commands Available in "Databricks CLI"?
# MAGIC * In the "Command Prompt", type the Utility Command "databricks --help" to See All the Utility Commands that are available in the "Databricks CLI".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to See the Syntax of a Particular Command in "Databricks CLI"?
# MAGIC * In the "Command Prompt", type the Utility Command "databricks 'command-name' --help" to see the Available Parameters and Settings of that Particular Utility Command of the "Databricks CLI".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to "Connect" to a Particular "Azure Databricks Workspace" from "Databricks CLI"?
# MAGIC * Using the Utility Command "configure", it is possible to "Configure" a Particular "Azure Databricks Workspace" to "Connect" to that "Azure Databricks Workspace" using "Token".
# MAGIC * Step 1: Type the Utility Command "databricks configure --token" in the "Comand Prompt" and press "Enter".
# MAGIC * Step 2: Upon pressing "Enter", it will ask to provide the "Databricks Host". To get the "Databricks Host", open the "Azure Databricks Workspace" and "Copy" the URL from the "Address Bar" of the "Browser" till the "Question Mark" (?).
# MAGIC * Step 3: Type the copied URL for the "Databricks Host" and press "Enter".
# MAGIC * Step 4: Upon pressing "Enter", it will ask to provide the "Token" associated with the provided "Databricks Host". To get the "Token", perform the following steps -
# MAGIC * Step 4.1: Open the "Azure Databricks Workspace" and click on "Settings" from the "Left Menu" of the "Screen".
# MAGIC * Step 4.2: Click on the "User Settings" menu option.
# MAGIC * Step 4.3: In the opened "User setings" page, click on "Generate new token" button.
# MAGIC * Step 4.4: Provide a meaningful text in the Textbox "Comment", provide the desired value in the Textbox "Lifetime (days)" to declare "For How Many Days" the "Token" to be "Generated" needs to be "Valid". Finally, click on the "Generate" buton.
# MAGIC * Step 4.5: Copy the generated "Token" and "Store" it in a File.
# MAGIC * Step 5: Go back to the "Command Prompt" where the "Databricks CLI" is opened, and, type the copied "Token" and press "Enter".
# MAGIC * Step 6: To Verify if the provided "Databricks Host" URL and the "Token" was Captured by the "Databricks CLI" correctly, navigate to the Windows Explorer path "C:\Users\'user'\". In this path, there will be a File called "databrickscfg" for "Databricks Workspace Configuration" in the "Local Machine". Perform a "Double Click" to open that File.
# MAGIC * Step 7: Verify if the Values of the Properties "host" and "token" are desired. If not, paste the correct Values. Save the File and Close it.

# COMMAND ----------

# MAGIC %md
# MAGIC # Access DBFS Root of an "Azure Databricks Workspace" Using "Databricks CLI" from a "Local Machine"
# MAGIC * In the "Command Prompt", type the Utility Command "databricks fs ls" to Display "All" the Available "Directories" in the DBFS Root.
# MAGIC * In the "Command Prompt", type the Utility Command "databricks fs ls dbfs:'path'" to Display "All" the Available "Sub-Directories" and the "Files" present in the provided "Directory".
