# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 1
# MAGIC * Topic: Walkthrough of the "Dbutils" API to Access the DBFS
# MAGIC * Author: Oindrila Chakraborty

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

# DBTITLE 1,cp - Copies a File or Directory Across FileSystem
dbutils.fs.cp("dbfs:/tmp/parentDir/childDir/firstFile.csv", "dbfs:/FileStore/tables/retailer/copyOfFirstFile.csv")

# COMMAND ----------

# DBTITLE 1,mv - Moves a File or Directory Across FileSystem
# A "move" is a "copy" followed by a "delete", even for moves within filesystems.
dbutils.fs.mv("dbfs:/tmp/parentDir/childDir", "dbfs:/FileStore/tables/retailer/image/", True)

# COMMAND ----------

# DBTITLE 1,head - Display Contents of File
# If a "Number" is specified, the "head ()" method returns up to the mentioned number of bytes of the given file.
dbutils.fs.head("/FileStore/tables/retailer/image/firstFile.csv", 7)

# COMMAND ----------

# DBTITLE 1,head - Display Contents of File
# If no "Number" is specified, the "head ()" method returns the maximum bytes of the given file.
dbutils.fs.head("/FileStore/tables/retailer/image/firstFile.csv")

# COMMAND ----------

# DBTITLE 1,rm - Removes a File or Directory
# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/copyOfFirstFile.csv")

# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/image/firstFile.csv")

# Delete "File"
dbutils.fs.rm("/FileStore/tables/retailer/image/secondFile.csv")

# Delete "Directory"
dbutils.fs.rm("dbfs:/tmp/parentDir/childDir")

# Delete "Directory"
dbutils.fs.rm("/tmp/parentDir")

# Delete "Directory"
dbutils.fs.rm("/tmp")

# Delete File
dbutils.fs.rm("dbfs:/FileStore/tables/retailer/copyOfFirstFile.csv")
