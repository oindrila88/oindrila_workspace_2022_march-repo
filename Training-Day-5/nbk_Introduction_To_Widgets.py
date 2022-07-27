# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 5
# MAGIC * Topic: Introduction to "Widgets" in "Databricks".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Input Widgets"?
# MAGIC * "Input Widgets" allow to "Add Parameters" to the "Notebooks".
# MAGIC * The "Widget API" consists of "Calls" to "Create" Various Types of "Input Widgets", "Remove" those "Widgets", and "Get Bound Values".
# MAGIC * "Databricks Widgets" are best for -
# MAGIC * A. Building a "Notebook" that is "Re-Executed" with Different "Parameters".
# MAGIC * B. Quickly "Exploring Results" of a "Single Query" with "Different Parameters".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Databricks Widget" Types
# MAGIC * There are 4 types of "Widgets" -
# MAGIC * A. text - "Input" a "Value" in a "Textbox".
# MAGIC * B. dropdown: "Select" a "Value" from a "List" of Provided "Values".
# MAGIC * C. combobox: "Combination" of "Text" and "Dropdown". "Select" a "Value" from a Provided "List" or "Input" One in the "Textbox".
# MAGIC * D. multiselect: "Select" One or More "Values" from a "List" of Provided "Values".
# MAGIC * "Widget Dropdowns" and "Textboxes" appear "Immediately" following the "Notebook Toolbar".

# COMMAND ----------

# DBTITLE 1,Create a "Dropdown Widget" with "Options" Starting from "1" to "10" and with "Default Value" as "11"
dbutils.widgets.dropdown("Dropdown Widget", "11", [str(x) for x in range(11, 20)])

# COMMAND ----------

# DBTITLE 1,Create a "Text" Widget with Value "Oindrila"
dbutils.widgets.text("Text Widget", "Oindrila")

# COMMAND ----------

# DBTITLE 1,Fetch the "Value" of the "Text" Widget
textWidgetValue = dbutils.widgets.get("Text Widget")
print(textWidgetValue)

# COMMAND ----------

# DBTITLE 1,Fetch the "Current Selected Value" of the "Dropdown" Widget
dropdownWidgetSelectedValue = dbutils.widgets.get("Dropdown Widget")
print(dropdownWidgetSelectedValue)

# COMMAND ----------

# DBTITLE 1,"Remove" a Particular "Widget"
# "Remove" the "Dropdown" Widget.
dbutils.widgets.remove("Dropdown Widget")

# COMMAND ----------

# DBTITLE 1,"Remove" All the "Widgets"
dbutils.widgets.removeAll()
