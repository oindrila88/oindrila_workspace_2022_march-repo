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

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Medallion Architecture"?
# MAGIC * Typically "CDC" is "Used" in an "Ingestion" to "Analytics Architecture", called the "Medallion Architecture".
# MAGIC * The "Medallion Architecture" takes "Raw Data" "Landed" from the "Source Systems" and "Refines" the "Data" through the "Bronze", "Silver" and "Gold" "Tables".
# MAGIC * "CDC" and the "Medallion Architecture" provide "Multiple Benefits" to "Users" since the "Only Changed" or "Added Data" needs to be "Processed".
# MAGIC * In addition, the "Different Tables" in the "Architecture" "Allow" the "Different Personas", such as "Data Scientists" and "BI Analysts", to "Use" the "Correct Up-To-Date Data" for their "Needs".
# MAGIC * <img src = '/files/tables/images/cdc_1.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Change Data Feed"?
# MAGIC * "Change Data Feed" ("CDF") feature "Allows" the "Delta Tables" to "Track" the "Row-Level Changes" between the "Versions" of a "Delta Table".
# MAGIC * With "Change Data Feed" ("CDF") "Enabled" on a "Delta Table", the "Runtime Records" "Change Events" for "All" the "Data" "Written" into the "Delta Table". This "Includes" the "Row" of "Data" "Along With" the "Metadata" "Indicating" whether the "Specified Row" was "Inserted", "Deleted", or "Updated".
# MAGIC * It is possible to "Read" the "Change Events" in the "Batch Queries" using the "SQL" and "DataFrame APIs", i.e., "df.read".
# MAGIC * It is possible to "Read" the "Change Events" in the "Streaming Queries" using the "DataFrame APIs", i.e., "df.readStream".
# MAGIC * "CDF" "Captures" the "Changes Only" from a "Delta Table" and is "Only Forward-Looking" "Once Enabled".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Use cases" of "Change Data Feed"
# MAGIC * "Change Data Feed" is "Not Enabled" "By Default". The following "Use Cases" "Drive" "When" the "Change Data Feed"should be "Enabled".
# MAGIC * A. <b>Silver and Gold Tables</b>: "Improve" the "Delta Performance" by "Processing" the "Only Row-Level Changes" following "Initial" "MERGE", "UPDATE", or "DELETE" Operations to "Accelerate" and "Simplify" the "ETL" and "ELT" Operations.
# MAGIC * B. <b>Materialized Views</b>: "Create" the "Up-To-Date", "Aggregated Views" of "Information" for "Use" in "BI" and "Analytics" "Without" having to "Re-Process" the "Full Underlying Tables", Instead "Updating" only where "Changes" have "Come Through".
# MAGIC * C. <b>Transmit Changes</b>: "Send" a "Change Data Feed" to "Downstream Systems", such as "Kafka" or "RDBMS" that can "Use" it to "Incrementally Process" in the "Later Stages" of "Data Pipelines".
# MAGIC * D. <b>Audit Trail Table</b>: "Capture" the "Change Data Feed" as a "Delta Table" provides "Perpetual Storage" and "Efficient Query Capability" to "See" "All" the "Changes" "Over Time", including when "Deletes" "Occur" and what "Updates" were "Made".
