# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to "Databricks File System" (DBFS)
# MAGIC * Topic: Walkthrough of the "Dbutils.fs" API to Access the DBFS
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Databricks Utilities"
# MAGIC * "<b>Databricks Utilities</b>" ("<b>dbutils</b>") make it "Easy" to "Perform" the "Powerful Combinations" of "Tasks".
# MAGIC * It is possible to "Use" the "<b>Databricks Utilities</b>" to "Work" with the "<b>Object Storage</b>" efficiently, to "<b>Chain</b>" and "<b>Parameterize</b>" the "<b>Notebooks</b>", and to "Work" with the "<b>Secrets</b>".
# MAGIC * "<b>Databricks Utilities</b>" ("<b>dbutils</b>") are "Not Supported" "Outside" of the "Notebooks".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is the "Databricks File System" (DBFS)
# MAGIC * The "Databricks File System" (DBFS) is a "Distributed File System" that is "Mounted" into a "Databricks Workspace" and "Available" on "Databricks Clusters".<br>"DBFS" is an "Abstraction" on "Top" of "Scalable Object Storage" that "Maps" the "Unix-Like File System Calls" to "Native Cloud Storage API Calls".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is the "DBFS Root"
# MAGIC * The "DBFS Root" is the "Default Storage Location" for a "Databricks Workspace", that is "Provisioned" as "Part" of "Databricks Workspace Creation" in the "Cloud Account" Containing the "Databricks Workspace".
# MAGIC * The "DBFS Root" "Contains" a "Number" of "Special Locations" that "Serve" as "Defaults" for "Various Actions" that are "Performed" by the "Users" in the "Workspace".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Difference" Between "Databricks File System" (DBFS) and "DBFS Root"
# MAGIC * "DBFS" is a "File System" that is "Used" for "Interacting" with "Data" in the "Cloud Object Storage", while, the "DBFS Root" is a "Cloud Object Storage Location" itself.
# MAGIC * "DBFS" is "Used" to "Interact" with the "DBFS Root".
# MAGIC * "DBFS" has "Many Other Applications" beyond the "DBFS Root".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Default Directories" Present in the "DBFS Root"
# MAGIC * Each "Databricks Workspace" has several "Directories" that are "Configured" in the "DBFS Root Storage Container" by default. Some of these "Directories" link to the "Locations" on the "DBFS Root", while other "Directories" are "Virtual Mounted".
# MAGIC * Following are the "Default Directories" Present in a "Databricks Workspace" -
# MAGIC * A. <b>/FileStore</b>: "Data" and "Libraries" that are "Uploaded" through the "Databricks UI" go to the "<b>/Filestore</b>" Location by default. "Generated Plots" are also "Stored" in this "Directory".
# MAGIC * B. <b>/databricks-datasets</b>: "Databricks" provides a "Number of Open Source Datasets" in the "<b>/databricks-datasets</b>" Directory. Many of the "Tutorials" and "Demos" provided by "Databricks" actually "Reference" these "Datasets", which can be "Used" indepedently "Explore" the "Functionality" of "Databricks".<br>This is a "Virtual Mounted Directory". If the "Users" are "Unable" to "Access" the "Data" in this "Directory", it is advisable to "Contact" the "Workspace Administrator".
# MAGIC * C. <b>/databricks-results</b>: "<b>/databricks-results</b>" "Stores" the "Files", which are "Generated" by "Downloading" the "Full Results" of a "Query".<br>This is a "Virtual Mounted Directory". If the "Users" are "Unable" to "Access" the "Data" in this "Directory", it is advisable to "Contact" the "Workspace Administrator".
# MAGIC * D. <b>/databricks/init</b>: The "<b>/databricks/init</b>" Directory "Contains" the "Global Init Scripts".
# MAGIC * E. <b>/user/hive/warehouse</b>: "Databricks" "Stores" the "Data" of the "Managed Tables" that are "Created" in the "<b>hive_metastore</b>" in the "<b>/user/hive/warehouse</b>" Directory by default.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Init Script"
# MAGIC * An "Init Script" is a "Shell Script" that "Runs" during the "Startup" of each "Cluster Node" before the "Apache Spark Driver" or "Worker JVM" "Starts".
# MAGIC * Some "Examples" of "Tasks" "Performed" by the "Init Scripts" include -
# MAGIC <br>"Install Packages" and "Libraries" not "Included" in "Databricks Runtime". To "Install" the "Python Packages", the "Databricks" "pip" "Binary" should be "Used", which is "Located" at "<b>/databricks/python/bin/pip</b>" to "Ensure" that "Python Packages" are "Installed" into the "Databricks Python Virtual Environment", rather than the "System Python Environment". For example, "/databricks/python/bin/pip install 'package-name'".
# MAGIC <br>"Modify" the "JVM System Classpath" in "Special Cases".
# MAGIC <br>"Set" the "System Properties" and "Environment Variables" that are "Used" by the "JVM".
# MAGIC <br>"Modify" the "Spark Configuration Parameters".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Types" of "Init Scripts"
# MAGIC * "Databricks" "Supports" the following "Two" Types of "Init Scripts" -
# MAGIC * A. <b>Cluster-Scoped</b>: "<b>Cluster-Scoped</b>" "Init Scripts" "Run" on "Every Cluster", which is "Configured" with the "Script". This is the "Recommended Way" to "Run" an "Init Script".
# MAGIC * B. <b>Global</b>: "<b>Global</b>" "Init Scripts" "Run" on "Every Cluster" in the "Workspace". "<b>Global</b>" "Init Scripts" can help to "Enforce" the "Consistent Cluster Configurations" across the "Databricks Workspace".
# MAGIC <br>The "<b>Global</b>" "Init Scripts" should be "Used" carefully because these can cause "Unanticipated Impacts", like "Library Conflicts".
# MAGIC <br>Only "Admin Users" can "Create" the "<b>Global</b>" "Init Scripts".
# MAGIC * <b>Warning!</b>: "Databricks" "Scans" the "Reserved Location", i.e., "<b>/databricks/init</b>" for the "<b>Legacy Global</b>" "Init Scripts", which are "Enabled" in "New Workspaces" by default. "Databricks" "Recommends" to "Avoid" "Storing" the "Init Scripts" in this "Directory" to "Avoid" the "Unexpected Behavior".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "File System Utility"
# MAGIC * The "File System Utility" "Allows" to "Access" "What" is the "Databricks File System" (DBFS), making it "Easier" to "Use" the "Databricks" as a "File System".

# COMMAND ----------

# DBTITLE 1,dbutils.fs.help() - To "List" the "Available Commands" Present in the "File System Utility"
dbutils.fs.help()

# COMMAND ----------

# DBTITLE 1,ls - List Content of Specified Directory
dbutils.fs.ls("dbfs:/FileStore/tables/retailer/data")

# COMMAND ----------

# DBTITLE 1,mkdirs - Create Directory (if Not Exists). Also Creates Necessary Parent Directories
dbutils.fs.mkdirs("dbfs:/tmp/parentDir/childDir")

# COMMAND ----------

# DBTITLE 1,put - Write String to a Specified File in a Path
#  If the file exists, it will be overwritten.
dbutils.fs.put("dbfs:/tmp/parentDir/childDir/firstFile.csv", "Hi | I am Oindrila | Chakraborty | Bagchi", True)
dbutils.fs.put("dbfs:/tmp/parentDir/childDir/secondFile.csv", "I | Live | In | Kolkata", True)

# COMMAND ----------

# DBTITLE 1,cp - Copies a File or Directory Across FileSystems
dbutils.fs.cp("dbfs:/tmp/parentDir/childDir/firstFile.csv", "dbfs:/FileStore/tables/retailer/copyOfFirstFile.csv")

# COMMAND ----------

# DBTITLE 1,mv - Moves a File or Directory Across FileSystems
# A "move" is a "copy" followed by a "delete", even for moves within filesystems.
dbutils.fs.mv("dbfs:/tmp/parentDir/childDir", "dbfs:/FileStore/tables/retailer/image/", True)

# COMMAND ----------

# DBTITLE 1,head - Display Contents of File
# If a "Number" is specified, the "head ()" method returns up to the mentioned number of bytes of the given file.
dbutils.fs.head("/tmp/parentDir/childDir/firstFile.csv", 10)

# COMMAND ----------

# DBTITLE 1,head - Display Contents of File
# If no "Number" is specified, the "head ()" method returns the maximum bytes of the given file.
dbutils.fs.head("/tmp/parentDir/childDir/firstFile.csv")

# COMMAND ----------

# DBTITLE 1,rm - Removes a File or Directory
# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/copyOfFirstFile.csv")

# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/image/firstFile.csv")

# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/image/secondFile.csv")

# Delete "Directory"
dbutils.fs.rm("/tmp/parentDir")
