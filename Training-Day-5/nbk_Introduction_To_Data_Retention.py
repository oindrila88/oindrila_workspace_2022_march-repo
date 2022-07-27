# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Introduction to "Data Retention" of a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Data Retention" of a "Delta Table"
# MAGIC * To "Time Travel" to a "Previous Version", both the "Log Files" and the "Data Files" for that "Version" must be "Retained".
# MAGIC * The "Data Files" that are "Backing" a "Delta Table" are "Never Deleted Automatically". The "Data Files" are "Deleted" only when the "VACUUM" Command is "Run" or when the User "Deletes" the "Data Files" Manually.
# MAGIC * The "VACUUM" Command does not "Delete" the "Delta Log Files".  The "Delta Log Files" are "Automatically Cleaned Up" after "Checkpoints" are "Written".
# MAGIC * By default, it is possible to "Time Travel" to a "Delta Table" up to "30 days old" Unless the following have been done on that "Delta Table" -
# MAGIC * A. The "VACUUM" Command is "Run" on the "Delta Table".
# MAGIC * B. The "Retention Periods" of the "Data Files" or "Log Files" are "Changed"  using the following "Table Properties" -
# MAGIC * i. delta.logRetentionDuration = "interval 'interval'": This Property Controls "How Long" the "History" for a "Delta Table" is "Kept". The "Default" is "Interval 30 Days".
# MAGIC * Each time a "Checkpoint" is "Written", "Azure Databricks" "Automatically Cleans Up" the "Log Entries" that are "Older Than" the "Retention Interval".
# MAGIC * If this Property is "Set" to a "Large Enough Value", then "Many Log Entries" are "Retained". This "Should Not Impact" the "Performance" as "Operations" against the "Log" are "Constant Time". "Operations" on "History" are "Parallel" but will become "More Expensive" as the "Log Size" Increases.
# MAGIC * ii. delta.deletedFileRetentionDuration = "interval 'interval'": This Property Controls "How Long Ago" a "Data File" must have been "Deleted" before being a "Candidate" for "VACUUM". The "Default" is "Interval 7 Days".
# MAGIC * To "Access" the "30 Days" of "Historical Data" even if the "VACUUM" Command is "Run" on the "Delta Table", the Property "delta.deletedFileRetentionDuration" must be "Set" to "Interval 30 Days". This "Setting" may cause the "Storage Costs" to "Go Up".
