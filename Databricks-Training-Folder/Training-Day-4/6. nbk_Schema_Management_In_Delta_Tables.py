# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: Introduction to "Schema Management" in "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Schema" is "Managed" in "Delta Tables"
# MAGIC * Every "Delta Table" in "Databricks" Contains a "Schema", which is a "Blueprint" that "Defines" the "Shape" of the Data, such as "Data Types" and "Columns", and "Metadata". With "Delta Lake", the "Table’s Schema" is "Saved" in "JSON Format" Inside the "Transaction Log".
# MAGIC * As "Business Problems" and "Requirements" Evolve Over Time, so too does the "Structure" of the Data. With "Delta Lake", as the Data "Changes", "Incorporating" the "New Dimensions" is "Easy". "Users" have "Access" to "Simple Semantics" to "Control" the "Schema" of the "Delta Tables" in the following two ways -
# MAGIC * <b>A. Schema Enforcement:</b> The "Schema Enforcement" Prevents the "Users" from "Accidentally Polluting" the "Delta Tables" with "Mistakes" or "Garbage Data".
# MAGIC * <b>B. Schema Evolution:</b> The "Schema Evolution" Enables the "Users" to "Automatically Add New Columns" of "Rich Data" when those "New Columns" Belong.
