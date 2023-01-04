# Databricks notebook source
# MAGIC %md
# MAGIC # Chain Notebook Call
# MAGIC * Topic: How to Call "One Notebook" from "Another Notebook"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Ways to "Modularize" or "Link Notebooks"
# MAGIC * A. Using "%run ()" - The "%run ()" Command allows to "Include" "Another Notebook" Within a "Notebook", i.e., the "%run" Command "Invokes" "Another Notebook" in the "Same Notebook Context".
# MAGIC <br>It is possible to Use "%run ()" to "Modularize" the "Code". Example - By Putting "Supporting Functions" in a "Separate Notebook".
# MAGIC <br>When the "%run ()" Command is "Run", the "Called Notebook" is Immediately "Executed" and the "Functions" and "Variables" defined in it Become Available in the "Calling Notebook".
# MAGIC <br>This "Method" is "Suitable" to "Define" a "Notebook" that "Holds" "All" the "Constants", "Variables" or a "Centralized Shared Function Library", which "Needs" to be "Referred" in the "Calling Notebook".
# MAGIC * B. dbutils.notebook.run () - To Implement "Notebook Workflows", the "dbutils.notebook.*" Methods are "Used".
# MAGIC <br>Unlike "%run ()" Command, the "dbutils.notebook.run()" Method Starts a "New Job" to "Run" the "Notebook", i.e., this "Method" will "Run" "Another Notebook" in a "New Notebook Context".

# COMMAND ----------

# DBTITLE 1,Call "One Notebook" from "Another Notebook" Using "%run ()"
# MAGIC %run
# MAGIC ./1._nbk_Parameterize_Notebooks $Text_Widget="Soumyajyoti Bagchi" $Dropdown_Widget="15"

# COMMAND ----------

# DBTITLE 1,Display the Value of the Variable "dropdownWidgetSelectedValue", Set in the Notebook "1._nbk_Parameterize_Notebooks"
print(dropdownWidgetSelectedValue)

# COMMAND ----------

# DBTITLE 1,Display the Value of the Variable "textWidgetSelectedValue", Set in the Notebook "1._nbk_Parameterize_Notebooks"
print(textWidgetSelectedValue)

# COMMAND ----------

# DBTITLE 1,Call "One Notebook" from "Another Notebook" Using "dbutils.notebook.run ()"
dbutils.notebook.run("1._nbk_Parameterize_Notebooks", 60, {"Text_Widget": "Oindrila Chakraborty", "Dropdown_Widget": "14"})
