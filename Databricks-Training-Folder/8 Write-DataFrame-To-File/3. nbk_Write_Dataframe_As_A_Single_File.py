# Databricks notebook source
# MAGIC %md
# MAGIC # Write the Contents of a DataFrame as a Single File in Databricks
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Perform Some Transformation on that DataFrame and Create New DataFrame. Write the New DataFrame into a Single File and Store into DBFS.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a DAT File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerDatFileUsingCsv = spark.read\
                                  .option("sep", "|")\
                                  .option("header", "true")\
                                  .csv("dbfs:/FileStore/tables/retailer/data/customer.dat")

display(df_ReadCustomerDatFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Find "Total Number of Rows" in the Created DataFrame
df_ReadCustomerDatFileUsingCsv.count()

# COMMAND ----------

# DBTITLE 1,Delete All the Files Present in the Path "dbfs:/tmp/temp_output_csv"
dbutils.fs.rm("dbfs:/tmp/temp_output_csv", True)

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in the "Temporary Path" Using "coalesce(1)"
# Write the "Contents" of the "DataFrame" as a "Single CSV File" in the "Temporary Path" Using "coalesce(1)" on the "DataFrame".
df_ReadCustomerDatFileUsingCsv.coalesce(1)\
                              .write\
                              .format("csv")\
                              .options(\
                                       path = "dbfs:/tmp/temp_output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,View the Number of Files Present in the Path "dbfs:/tmp/temp_output_csv"
dbutils.fs.ls("dbfs:/tmp/temp_output_csv")

# COMMAND ----------

# DBTITLE 1,Find the Name of the "CSV File" from "All the Files" in the Path "dbfs:/tmp/temp_output_csv"
tempFilePath ="dbfs:/tmp/temp_output_csv"

allTempFiles = dbutils.fs.ls(tempFilePath)
csvFileName = ""
for tempFile in allTempFiles:
    if tempFile.name.endswith(".csv"):
        csvFileName = tempFile.name

print("CSV File Name : " + csvFileName)

# COMMAND ----------

# DBTITLE 1,Copy the "CSV File" from the Temporary Path "dbfs:/tmp/temp_output_csv" To the Final Path "dbfs:/tmp/output_csv"
from datetime import datetime

currentHour = datetime.now().hour
currentMinute = datetime.now().minute
currentSecond = datetime.now().second

temporaryPathWithFileName = tempFilePath + "/" + csvFileName
finalTargetPathWithFileName = f"dbfs:/tmp/output_csv/Demo_CSV_{currentHour}_{currentMinute}_{currentSecond}.csv"

dbutils.fs.cp(temporaryPathWithFileName, finalTargetPathWithFileName)

# COMMAND ----------

# DBTITLE 1,Verify If the "CSV File" Got Created in the "Target Path", i.e., "dbfs:/tmp/output_csv/"
dbutils.fs.ls("dbfs:/tmp/output_csv/")

# COMMAND ----------

# DBTITLE 1,Delete "All the Files" Present in the Temporary Path "dbfs:/tmp/temp_output_csv"
dbutils.fs.rm(tempFilePath, True)

# COMMAND ----------

# DBTITLE 1,Verify If "All the Files" Present in the Temporary Path "dbfs:/tmp/temp_output_csv" are Deleted
dbutils.fs.ls(tempFilePath)
