# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Remove "Unused Data Files" that are "No Longer" referenced from a "Table Directory".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "VACCUM" Command on "Delta Table"
# MAGIC * It is possible to "Recursively Remove" the "Unused Data Files", by "Running" the "VACCUM" command on the "Delta Table", that are -
# MAGIC * A. "No Longer Referenced" by a "Delta Table".
# MAGIC * B. "No Longer" in the "Latest State" of the "Transaction Log" for the "Delta Table".
# MAGIC * C. "Older" than the "Retention Threshold".
# MAGIC * On "Delta Tables", "Azure Databricks" does "Not Automatically Trigger" the "VACUUM" Operations.
# MAGIC * "Data Files" of the "Delta Table" are "Deleted" according to the "Time" the "Data Files" have been "Logically Removed" from Delta’s "Transaction Log" plus "Retention Hours", not the "Modification Timestamps" on the "Storage System". The "Default Retention Threshold" for the "Files" is "7 Days".
# MAGIC * The "VACCUM" Command "Removes" "All" the "Data Files" from the "Table Directories" that are "Not Managed" by "Delta Lake", "Ignoring" the "Directories" beginning with an Underscore "_", which includes the "_delta_log". If "Additional Metadata", like - "Structured Streaming Checkpoints" are "Stored" within a "Delta Table Directory", use a "Directory" name such as "_checkpoints".
# MAGIC * The "VACCUM" Command "Deletes" "Only" the "Data Files", and, "Not" the "Log Files". The "Log Files" are "Deleted" Automatically and Asynchronously after "Checkpoint Operations".
# MAGIC * The "Default Retention Period" of the "Log Files" is "30 Days", which can be "Configurable" through the "delta.logRetentionDuration" Property that can be "Set" with the "ALTER TABLE SET TBLPROPERTIES" SQL Command.
# MAGIC * The "Ability" to "Time Travel" back to a "Version" that are "Older" than the "Retention Period" is "Lost" after Running the "VACCUM" Command.

# COMMAND ----------

# MAGIC %md
# MAGIC # Perform Chcek Before Running "VACCUM" on "Delta Table"
# MAGIC * It is recommended to "Set" a "Retention Interval" to be at least "7 Days", because "Old Snapshots" and "Uncommitted Files" can still be in Use by "Concurrent Readers" or "Writers" to the "Delta Table".
# MAGIC * If the "VACUUM" Command "Cleans Up" the "Active Files", the "Concurrent Readers" can "Fail" or, worse, the "Delta Tables" can be "Corrupted" when the "VACUUM" Command "Deletes" the "Data Files" that have "Not" yet been "Committed".
# MAGIC * An "Interval" must be "Chosen" that is "Longer" than the "Longest Running Concurrent Transaction" and the "Longest Period" that any "Stream" can "Lag Behind" the most "Recent Update" to the "Delta Table".

# COMMAND ----------

# DBTITLE 1,Use "VACCUM" on a "Managed Delta Table"
# MAGIC %sql
# MAGIC VACUUM retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# DBTITLE 1,Use "VACCUM" on a "Managed Delta Table" to "Delete" the "Table Directory Files" that are "Older than a Retention Threshold" 
# MAGIC %sql
# MAGIC VACUUM retailer_db.tbl_DeltaCustomerFromDataFrame RETAIN 240 HOURS

# COMMAND ----------

# DBTITLE 1,Use "VACCUM" on a "Managed Delta Table" to "Delete" the "Table Directory Files" and Return the "List of Deleted Files"
# MAGIC %sql
# MAGIC VACUUM retailer_db.tbl_DeltaCustomerFromDataFrame DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC # "Safety Check" to "Prevent" from "Running" a dangerous "VACUUM" Command
# MAGIC * "Delta Lake" has a "Safety Check" to "Prevent" from "Running" a dangerous "VACUUM" Command.
# MAGIC * If it is certain that there are "No Operations" being "Performed" on the "Delta Table" that take "Longer" than the "Retention Interval", it is possible to "Turn Off" the "Safety Check" by "Setting" the "Spark configuration" Property "spark.databricks.delta.retentionDurationCheck.enabled" to "False".

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM retailer_db.tbl_DeltaCustomerFromDataFrame RETAIN 4 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "VACCUM" Command on "Apache Spark Table"
# MAGIC * The "VACCUM" Command "Recursively Removes" the "Directories" associated with the "Spark Table" and "Remove" the "Uncommitted Files" that are "Older" than a "Retention Threshold". The "Default Threshold" is "7 Days".
# MAGIC * On "Spark Tables", "Azure Databricks" "Automatically Triggers" the "VACUUM" Operations as "Data" is "Written".
