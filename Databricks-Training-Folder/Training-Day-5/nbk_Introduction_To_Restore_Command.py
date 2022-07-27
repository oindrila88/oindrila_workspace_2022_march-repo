# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Introduction to "Restore" Command on a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Restore" a "Delta Table" to an "Earlier State"
# MAGIC * It is possible to "Restore" a "Delta Table" to its "Earlier State" by using the "RESTORE" Command.
# MAGIC * A "Delta Table" Internally Maintains "Historic Versions" of the "Table" that Enable it to be "Restored" to an "Earlier State".
# MAGIC * A "Version" Corresponding to the "Earlier State" or a "Timestamp" of When the "Earlier State" was "Created" are "Supported" as "Options" by the "RESTORE" Command.
# MAGIC * It is possible to "Restore" an "Already Restored Table".
# MAGIC * It is possible to "Restore" a "Cloned Table".
# MAGIC * "Restoring" a "Delta Table" to an "Older Version" where the "Data Files" were "Deleted Manually" or by "VACUUM" Command will "Fail". "Restoring" to this "Version" is still "Partially" Possible if the Property "spark.sql.files.ignoreMissingFiles" is "Set" to "True".
# MAGIC * The "Timestamp" Format for "Restoring" to an "Earlier State" is "yyyy-MM-dd HH:mm:ss". Providing Only a "date(yyyy-MM-dd)"" String is also "Supported".
# MAGIC * "Restore" is Considered a "Data-Changing Operation". "Delta Lake Log Entries" Added by the "RESTORE" Command contain "dataChange" Property "Set" to "True". If there is a "Downstream Application", such as a "Structured Streaming Job" that Processes the "Updates" to a "Delta Table", the "Data Change Log Entries" Added by the "Restore Operation" are Considered as "New Data Updates", and Processing the "New Data Updates" may result in "Duplicate Data".

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Restore a "Delta Table" Using "TIMESTAMP AS OF" Spark SQL Command
# MAGIC %sql
# MAGIC RESTORE cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame TO TIMESTAMP AS OF '2022-07-17 17:54:05'

# COMMAND ----------

# DBTITLE 1,Restore a "Delta Table" Using "timestampAsOf" Option in Python
from delta import *

DeltaTable.forName(spark, "cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame").restoreToTimestamp("2022-07-17 17:54:05")

# COMMAND ----------

# DBTITLE 1,Restore a "Delta Table" Using "VERSION AS OF" Spark SQL Command
# MAGIC %sql
# MAGIC RESTORE cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame TO VERSION AS OF 1

# COMMAND ----------

# DBTITLE 1,Restore a "Delta Table" Using "versionAsOf" Option in Python
from delta import *

DeltaTable.forName(spark, "cloned_retailer_db.tbl_Deep_Cloned_DeltaCustomerFromDataFrame").restoreToVersion(0)
