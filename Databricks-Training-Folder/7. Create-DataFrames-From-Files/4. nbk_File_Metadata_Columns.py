# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata Columns of Input Files in DataFrames
# MAGIC * Topic: Read Input File and Load the Data into a DataFrame with Metadata Columns
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # How to "Get" the "Metadata Information" of an "Input File"?
# MAGIC * It is possible to "<b>Get</b>" the "<b>Metadata Information</b>" for "<b>Input Files</b>" with the "<b>_metadata</b>" Column.
# MAGIC * The "<b>_metadata</b>" Column is a "<b>Hidden Column</b>", and, is "<b>Available</b>" for "<b>All Input File Formats</b>".
# MAGIC * To "<b>Include</b>" the "<b>_metadata</b>" Column in the "<b>Returned DataFrame</b>", it must be "<b>Explicitly Referenced</b>" in the "<b>Query</b>".
# MAGIC * The "<b>_metadata</b>" Column is "<b>Available</b>" in "<b>Databricks Runtime 10.5 and Above</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What Happens If "_metadata" Column is "Present" in the "Input File"?
# MAGIC * If the "<b>Input File</b>" "<b>Contains</b>" a "<b>Column</b>" named "<b>_metadata</b>", the "<b>Queries</b>" will "<b>Return</b>" the "<b>Column</b>" from the "<b>Input File</b>", and, "<b>Not</b>" the "<b>File Metadata</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Supported Metadata
# MAGIC * The "<b>_metadata</b>" Column is of "<b>Struct</b>" Data Type, "<b>Containing</b>" the following fields -
# MAGIC * <b>1</b>. <b>file_path</b>:
# MAGIC   * <b>Type</b>: String
# MAGIC   * <b>Description</b>: The "<b>File Path</b>" of the "<b>Input File</b>".
# MAGIC * <b>2</b>. <b>file_name</b>:
# MAGIC   * <b>Type</b>: String
# MAGIC   * <b>Description</b>: The "<b>Name</b>" of the "<b>Input File</b>", along with its "<b>Extension</b>".
# MAGIC * <b>3</b>. <b>file_size</b>:
# MAGIC   * <b>Type</b>: Long
# MAGIC   * <b>Description</b>: The "<b>Length</b>" of the "<b>Input File</b>", in "<b>Bytes</b>".
# MAGIC * <b>4</b>. <b>file_modification_time</b>:
# MAGIC   * <b>Type</b>: Timestamp
# MAGIC   * <b>Description</b>: The "<b>Last Modification Timestamp</b>" of the "<b>Input File</b>".

# COMMAND ----------

# DBTITLE 1,Read a CSV File and Create a DataFrame With Metadata Columns
df_ReadCustomerDatFileWithHeader = spark.read\
                                        .option("header", "true")\
                                        .option("sep", "|")\
                                        .format("csv")\
                                        .load("dbfs:/FileStore/tables/retailer/data/customer.dat")\
                                        .select("*", "_metadata")
      
display(df_ReadCustomerDatFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Read a CSV File and Create a DataFrame, and, Select Each of the Supported Inner Metadata Columns
df_ReadCustomerDatFileWithHeader = spark.read\
                                        .option("header", "true")\
                                        .option("sep", "|")\
                                        .format("csv")\
                                        .load("dbfs:/FileStore/tables/retailer/data/customer.dat")\
                                        .select(
                                                "*",
                                                "_metadata.file_path",
                                                "_metadata.file_name",
                                                "_metadata.file_size",
                                                "_metadata.file_modification_time"
                                               )
      
display(df_ReadCustomerDatFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Read a CSV File and Create a DataFrame, and, Use the Supported Inner Metadata Columns in "Filter"
# "Read" the CSV File "customer.dat" from the Folder Path "dbfs:/FileStore/tables/retailer/data/"
from pyspark.sql.functions import *

df_ReadCustomerDatFileWithHeader = spark.read\
                                        .option("header", "true")\
                                        .option("sep", "|")\
                                        .format("csv")\
                                        .load("dbfs:/FileStore/tables/retailer/data/*")\
                                        .select("*")\
                                        .filter(col("_metadata.file_name") == lit("customer.dat"))
      
display(df_ReadCustomerDatFileWithHeader)
