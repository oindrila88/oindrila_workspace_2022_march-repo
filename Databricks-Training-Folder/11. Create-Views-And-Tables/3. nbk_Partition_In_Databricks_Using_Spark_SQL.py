# Databricks notebook source
# MAGIC %md
# MAGIC # Partition of Tables in Databricks
# MAGIC * Topic: How to "Create", "Add" and "Drop" the "Partitions" in a "Table" Using Spark SQL in Databricks.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Partition"
# MAGIC * A "<b>Partition</b>" is "Composed" of a "Subset" of "Rows" in a "Table" that "Share" the "Same Value" for a "Predefined" "Subset of Columns", called the "<b>Partitioning Columns</b>".
# MAGIC <br>Using "<b>Partitions</b>" can "Speed Up" the "Queries" "Against" the "Table" as well as data manipulation.
