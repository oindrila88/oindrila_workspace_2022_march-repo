# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: Introduction to "Delta Lake".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Delta Lake"?
# MAGIC * "Delta Lake" is an "Open-Source Project" that enables building a "Lakehouse Architecture" on top of "Data Lakes".
# MAGIC * "Delta Lake" provides "ACID Transactions", "Scalable Metadata Handling", and Unifies "Streaming" and "Batch" Data Processing on top of existing "Data Lakes".

# COMMAND ----------

# MAGIC %md
# MAGIC # What Does "Delta Lake" Offers?
# MAGIC * ACID Transactions on Spark: Serializable Isolation Levels ensure that Readers never see "Inconsistent Data".
# MAGIC * Scalable Metadata Handling: Leverages Spark Distributed Processing Power to handle all the "Metadata" for "Petabyte-Scale Tables" with "Billions of Files" at ease.
# MAGIC * Streaming and Batch Unification: A "Table" in "Delta Lake" is a "Batch Table" as well as a "Streaming Source" and "Sink". "Streaming Data Ingest", "Batch Historic Backfill", "Interactive Queries" - all just work out of the box.
# MAGIC * Schema Enforcement: Automatically handles "Schema Variations" to prevent "Insertion of Bad Records" during "Ingestion".
# MAGIC * Time Travel: "Data Versioning" enables "Rollbacks", "Full Historical Audit Trails", and "Reproducible Machine Learning Experiments".
# MAGIC * Upserts and Deletes: Supports "Merge", "Update" and "Delete" Operations to enable Complex Use Cases like "Change-Data-Capture", "Slowly-Changing-Dimension" (SCD) Operations, "Streaming Upserts", and so on.
