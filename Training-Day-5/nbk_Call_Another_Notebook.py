# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Call "Another Notebook".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Ways to "Modularize" or "Link Notebooks"
# MAGIC * A. Using "%run ()" - The "%run ()" Command allows to "Include" "Another Notebook" Within a "Notebook".
# MAGIC * It is possible to Use "%run ()" to "Modularize" the "Code". Example - By Putting "Supporting Functions" in a "Separate Notebook".
# MAGIC * When the "%run ()" Command is "Run", the "Called Notebook" is Immediately "Executed" and the "Functions" and "Variables" defined in it Become Available in the "Calling Notebook".
# MAGIC * B. dbutils.notebook.run () - To Implement "Notebook Workflows", use the "dbutils.notebook.*" Methods. Unlike "%run ()", the "dbutils.notebook.run()" Method Starts a "New Job" to "Run" the "Notebook".

# COMMAND ----------

dbutils.notebook.run("nbk_Introduction_To_Widgets", 60, {"Text Widget": "Oindrila Chakraborty", "Dropdown Widget": "14"})
