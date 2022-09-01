# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 2
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Handle the NULL Values in a Single or Multiple Columns from that DataFrame.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # na.drop () -
# MAGIC * In PySpark, "pyspark.sql.DataFrameNaFunctions" class provides the "drop ()" functions to deal with NULL/None values.
# MAGIC * By using the "drop()" function it is possible to drop "All rows" with "NULL values" in "Any", "All", "Single", "Multiple", and "Selected" Columns.
# MAGIC * "drop()" function takes the following optional parameters -
# MAGIC * A. "how": This takes values ‘any’ or ‘all’.
# MAGIC *    By using "any", "drop" a "Row" if it contains "NULL Values" on "Any" Columns.
# MAGIC *    By using "all", "drop" a "Row" only if "All" Columns have "NULL Values". Default is "any".
# MAGIC * B. thresh: This takes "int" value to "drop" rows that have less than thresh hold "Non-NULL Values". Default is "None". This overwrites the "how" parameter.
# MAGIC * C. subset: Use this to "Select" the Columns for "NULL Values". Default is "None".
# MAGIC * By default, "drop()" funtion, without any arguments remove "All Rows" that have "NULL values" on "Any" Column of DataFrame.

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .option("inferSchema", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where No Rows Will Have NULL Values in All the Columns Using "na.drop" Function and "how = all"
df_DropCustomerRowsHavingNullInAllColumns = df_ReadCustomerFileUsingCsv.na.drop(how = "all")
display(df_DropCustomerRowsHavingNullInAllColumns)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where No Rows Will Have NULL Values in Any of the Columns Using "na.drop" Function and "how = any"
df_DropCustomerRowsHavingNullInAnyColumn = df_ReadCustomerFileUsingCsv.na.drop(how = "any")
display(df_DropCustomerRowsHavingNullInAnyColumn)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where Certain Columns Having NULL Values Are Dropped Using "na.drop" Function and "subset"
df_DropSelectedCustomerColumnsHavingNull = df_ReadCustomerFileUsingCsv.na.drop(subset = ["c_current_hdemo_sk", "c_first_name", "c_last_name"])
display(df_DropSelectedCustomerColumnsHavingNull)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where Each Row Contains Maximum of Two Columns With NULL Values Using "na.drop" Function and "thresh"
df_DropCustomerRowsHavingNullInMoreThanTwoColumns = df_ReadCustomerFileUsingCsv.na.drop(thresh = 2)
display(df_DropCustomerRowsHavingNullInMoreThanTwoColumns)

# COMMAND ----------

# MAGIC %md
# MAGIC # dropna () -
# MAGIC * In PySpark, the "dropna ()" method of "DataFrame" class is used to also deal with NULL/None values.
# MAGIC * By using the "dropna()" function it is possible to drop "All rows" with "NULL values" in "Any", "All", "Single", "Multiple", and "Selected" Columns.
# MAGIC * "drop()" function takes the following optional parameters -
# MAGIC * A. "how": This takes values ‘any’ or ‘all’.
# MAGIC *    By using "any", "drop" a "Row" if it contains "NULL Values" on "Any" Columns.
# MAGIC *    By using "all", "drop" a "Row" only if "All" Columns have "NULL Values". Default is "any".
# MAGIC * B. thresh: This takes "int" value to "drop" rows that have less than thresh hold "Non-NULL Values". Default is "None". This overwrites the "how" parameter.
# MAGIC * C. subset: Use this to "Select" the Columns for "NULL Values". Default is "None".
# MAGIC * By default, "dropna ()" method, without any arguments remove "All Rows" that have "NULL values" on "Any" Column of DataFrame.
# MAGIC * "DataFrame.dropna()" and "DataFrameNaFunctions.drop()" are "Aliases" of each other.

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where No Rows Will Have NULL Values in All the Columns Using "dropna" Method of DataFrame and "how = all"
df_DropCustomerRowsHavingNullInAllColumns = df_ReadCustomerFileUsingCsv.dropna(how = "all")
display(df_DropCustomerRowsHavingNullInAllColumns)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where No Rows Will Have NULL Values in Any of the Columns Using "dropna" Method of DataFrame and "how = any"
df_DropCustomerRowsHavingNullInAnyColumn = df_ReadCustomerFileUsingCsv.dropna(how = "any")
display(df_DropCustomerRowsHavingNullInAnyColumn)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where Certain Columns Having NULL Values Are Dropped Using "dropna" Method of DataFrame and "subset"
df_DropSelectedCustomerColumnsHavingNull = df_ReadCustomerFileUsingCsv.dropna(subset = ["c_current_hdemo_sk", "c_first_name", "c_last_name"])
display(df_DropSelectedCustomerColumnsHavingNull)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame Where Each Row Contains Maximum of Two Columns With NULL Values Using "dropna" Method of DataFrame and "thresh"
df_DropCustomerRowsHavingNullInMoreThanTwoColumns = df_ReadCustomerFileUsingCsv.dropna(thresh = 2)
display(df_DropCustomerRowsHavingNullInMoreThanTwoColumns)

# COMMAND ----------

# MAGIC %md
# MAGIC # na.fill () -
# MAGIC * In PySpark, "pyspark.sql.DataFrameNaFunctions" class provides the "fill ()" functions to "Replace" the NULL/None values on "All" or "Selected Multiple DataFrame Columns" with either zero(0), empty string, space, or any constant literal values.
# MAGIC * "fill ()" function takes the following parameters -
# MAGIC * A. value: "Value" should be the data type of "int", "long", "float", "string", or "dict". Value specified here will be replaced for NULL/None values.
# MAGIC * B. subset: This is optional, when used it should be the "Subset" of the "Column Names" where you wanted to "Replace" the NULL/None values.

# COMMAND ----------

# DBTITLE 1,Replace NULL of Any Columns of a DataFrame With Zero (0) Using "na.fill" Function
# All the Columns of "Integer"/"Long" Data Type Will Only be Replaced With Zero (0).

df_ReplaceNullWithZeroInAnyCustomerColumn = df_ReadCustomerFileUsingCsv.na.fill(value = 0)
display(df_ReplaceNullWithZeroInAnyCustomerColumn)

# COMMAND ----------

# DBTITLE 1,Replace NULL of Any Columns of a DataFrame With An Empty String ("") Using "na.fill" Function
# All the Columns of "String" Data Type Will Only be Replaced With Empty String ("").

df_ReplaceNullWithSpaceInAnyCustomerColumn = df_ReadCustomerFileUsingCsv.na.fill(value = "")
display(df_ReplaceNullWithSpaceInAnyCustomerColumn)

# COMMAND ----------

# DBTITLE 1,Replace NULL of Certain Columns of a DataFrame With A String Literal ("unknown") Using "na.fill" Function and "subset"
# All the Columns of "String" Data Type Will Only be Replaced With String Literal.

df_ReplaceNullWithStringInSelectedCustomerColumn = df_ReadCustomerFileUsingCsv.na.fill(value = "unknown", subset = ["c_salutation", "c_first_name", "c_last_name"])
display(df_ReplaceNullWithStringInSelectedCustomerColumn)

# COMMAND ----------

# MAGIC %md
# MAGIC # fillna () -
# MAGIC * In PySpark, the "fillna ()" method of "DataFrame" class is used to also "Replace" the NULL/None values on "All" or "Selected Multiple DataFrame Columns" with either zero(0), empty string, space, or any constant literal values.
# MAGIC * "fillna ()" method takes the following parameters -
# MAGIC * A. value: "Value" should be the data type of "int", "long", "float", "string", or "dict". Value specified here will be replaced for NULL/None values.
# MAGIC * B. subset: This is optional, when used it should be the "Subset" of the "Column Names" where you wanted to "Replace" the NULL/None values.
# MAGIC * * "DataFrame.fillna()" and "DataFrameNaFunctions.fill()" are "Aliases" of each other.

# COMMAND ----------

# DBTITLE 1,Replace NULL of Any Columns of a DataFrame With Zero (0) Using "fillna" Method of DataFrame
# All the Columns of "Integer"/"Long" Data Type Will Only be Replaced With Zero (0).

df_ReplaceNullWithZeroInAnyCustomerColumn = df_ReadCustomerFileUsingCsv.fillna(value = 0)
display(df_ReplaceNullWithZeroInAnyCustomerColumn)

# COMMAND ----------

# DBTITLE 1,Replace NULL of Any Columns of a DataFrame With An Empty String ("") Using "fillna" Method of DataFrame
# All the Columns of "String" Data Type Will Only be Replaced With Empty String ("").

df_ReplaceNullWithSpaceInAnyCustomerColumn = df_ReadCustomerFileUsingCsv.fillna(value = "")
display(df_ReplaceNullWithSpaceInAnyCustomerColumn)

# COMMAND ----------

# DBTITLE 1,Replace NULL of Certain Columns of a DataFrame With A String Literal ("unknown") Using "fillna" Method of DataFrame and "subset"
# All the Columns of "String" Data Type Will Only be Replaced With String Literal.

df_ReplaceNullWithStringInSelectedCustomerColumn = df_ReadCustomerFileUsingCsv.fillna(value = "unknown", subset = ["c_salutation", "c_first_name", "c_last_name"])
display(df_ReplaceNullWithStringInSelectedCustomerColumn)
