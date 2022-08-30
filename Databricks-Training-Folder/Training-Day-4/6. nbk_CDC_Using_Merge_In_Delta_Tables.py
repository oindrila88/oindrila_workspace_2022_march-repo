# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: Introduction to "Change Data Capture" ("CDC")
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Capture"?
# MAGIC * "Change Data Capture", or "CDC", in short, "Refers" to the "Process" of "Capturing" "Only" the "Changes" made in a "Set of Data Sources" since the "Last Successful Load" and "Merging" "Only" those "Changes" in a "Set of Target Tables", typically in a "Data Warehouse".
# MAGIC * The "Data Warehouse" is typically "Refreshed" "Nightly", "Hourly", or, in some cases, "Sub-Hourly" (e.g., "Every 15 Minutes"). This "Period" is "Refered" to as the "Refresh Period".
# MAGIC * The "Set" of "Changed Records" for a given "Table" "Within" a "Refresh Period" is "Referred" to as a "Change Set".
# MAGIC * Finally, the "Set of Records" "Within" a "Change Set" that has the "Same" "Primary Key" is "Referred" to as a "Record Set". Intuitively the "Record Set" "Refer" to "Different" "Changes" for the "Same Record" in the "Final Table".
