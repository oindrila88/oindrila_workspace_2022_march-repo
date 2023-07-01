# Databricks notebook source
# MAGIC %md
# MAGIC # Handle Bad Data Coming From File
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame and Handle the Bad Data Coming From that CSV File
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Bad Data While Reading File
# MAGIC * Typically there are "<b>Two Ways</b>" to "<b>Handle</b>" the "<b>Bad Data</b>" while "<b>Reading</b>" a "<b>File</b>" -
# MAGIC <br>1. "<b>Parsing Mode</b>"
# MAGIC <br>2. "<b>Bad Records Path</b>"

# COMMAND ----------

# DBTITLE 1,Display the "Bad Data" of the CSV File "household_demographics_m.dat"
# In the "hd_buy_potential" Column, the all the Values are supposed to be "Integer", but, some "Rows" contain "String" Values.
display(\
            spark.read\
                 .csv(path = "dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat", sep = "|", header = True)\
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Bad Data While Reading File Using "Parsing Mode"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parsing Mode
# MAGIC * When "<b>Reading Data</b>" from a "<b>File-Based Data Source</b>" with "<b>Specified Schema</b>", it is possible that the "<b>Data</b>" in the "<b>Files</b>" does "<b>Not Match</b>" with the "<b>Specified Schema</b>".
# MAGIC * The "<b>Consequences</b>" depend on the "<b>Mode</b>" that the "<b>Parser</b>" runs on. To "<b>Set</b>" the "<b>Mode</b>", the "<b>mode</b>" Option is used.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. "permissive" Mode
# MAGIC * In the "<b>permissive</b>" Mode, "<b>Each</b>" of the "<b>Rows</b>" having the "<b>Error Values</b>" will be "<b>Saved</b>" as a "<b>Different Column</b>" in the "<b>DataFrame</b>" in that "<b>Respective Row</b>", while "<b>Reading</b>" a "<b>File</b>", and, "<b>All</b>" the "<b>Error Values</b>" will be "<b>Replaced</b>" with "<b>NULL</b>" in the respective "<b>Columns</b>" of that "<b>Respective Row</b>".
# MAGIC * It is possible to "<b>Specify</b>" the "<b>Name</b>" of the "<b>Column</b>", where the "<b>Row</b>" having the "<b>Error Values</b>" will be "<b>Saved</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Schema" with a "Column" to "Store" "All" the "Rows" with "Error Values" for Loading a "DataFrame" from a "CSV" File
from pyspark.sql.functions import *
from pyspark.sql.types import *

# The Schema contains a "Special Column", i.e., "corrupt_data", which does "Not Exist" in the "Data" coming from the "CSV File". This "Special Column", i.e., "corrupt_data" "Captures" the "Rows" that did "Not Parse Correctly".
householdDemographicsSchema = StructType([
    StructField("hd_demo_sk", IntegerType(), True),
    StructField("hd_income_band_sk", IntegerType(), True),
    StructField("hd_buy_potential", IntegerType(), True),
    StructField("hd_dep_count", IntegerType(), True),
    StructField("hd_vehicle_count", IntegerType(), True),
    StructField("corrupt_data", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Usage of "permissive" Mode Option
# While "Creating" a "DataFrame" by "Reading" a "CSV File", the "permissive" Mode needs to be "Definied" in the "option ()" Method of "DataFrameReader" Class as "option("mode", "permissive")".
# In "permissive" Mode, another "Option" needs to be "Specified", i.e., "columnNameOfCorruptRecord" using the "option ()" Method of the "DataFrameReader" Class, while "Creating" a "DataFrame" by "Reading" a "CSV File", to "Specify" in "Which Column" the "Row Containing the Error Data" will be "Stored". The "Column Name" provided in the "Option", i.e., "columnNameOfCorruptRecord" should be "Present" in the "Schema" provided.
df_ReadHouseholDemographicsWithPermissiveMode = spark.read\
                                                     .option("mode", "permissive")\
                                                     .option("columnNameOfCorruptRecord", "corrupt_data")\
                                                     .csv(path = "dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat", sep = "|", header = True, schema = householdDemographicsSchema)
display(df_ReadHouseholDemographicsWithPermissiveMode)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. "dropmalformed" Mode
# MAGIC * In the "<b>dropmalformed</b>" Mode, "<b>All</b>" the "<b>Rows</b>" that have "<b>Error Values</b>" are actually "<b>Dropped Off</b>" from the created "<b>DataFrame</b>" , while "<b>Reading</b>" a "<b>File</b>".
# MAGIC * This Mode is "<b>Very Useful</b>", if "<b>Only</b>" the "<b>Good Data</b>" needs to be "<b>Stored</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Schema" with "No Column" to "Store" "All" the "Rows" with "Error Values" for Loading a "DataFrame" from a "CSV" File
from pyspark.sql.functions import *
from pyspark.sql.types import *

householdDemographicsSchema = StructType([
    StructField("hd_demo_sk", IntegerType(), True),
    StructField("hd_income_band_sk", IntegerType(), True),
    StructField("hd_buy_potential", IntegerType(), True),
    StructField("hd_dep_count", IntegerType(), True),
    StructField("hd_vehicle_count", IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Usage of "dropmalformed" Mode Option
# It can be clearly seen that "All" the "Rows" that have "Error Values" are actually "Dropped Off" from the created "DataFrame"
df_ReadHouseholDemographicsWithDropmalformedMode = spark.read\
                                                        .option("mode", "dropmalformed")\
                                                        .csv(path = "dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat", sep = "|", header = True, schema = householdDemographicsSchema)
display(df_ReadHouseholDemographicsWithDropmalformedMode)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. "failfast" Mode
# MAGIC * In the "<b>failfast</b>" Mode, while "<b>Reading</b>" a "<b>File</b>", whenever "<b>Any Row</b>" that has "<b>Error Value</b>" in it is "<b>Read</b>",  an "<b>Exception</b>" is "<b>Thrown</b>", and, "<b>Apache Spark</b>" "<b>Stops Reading</b>" the "<b>File Further</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "failfast" Mode Option
df_ReadHouseholDemographicsWithFailfastMode = spark.read\
                                                   .option("mode", "failfast")\
                                                   .csv(path = "dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat", sep = "|", header = True, schema = householdDemographicsSchema)
display(df_ReadHouseholDemographicsWithFailfastMode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Bad Data While Reading File Using "badrecordspath" Option

# COMMAND ----------

# MAGIC %md
# MAGIC * Using the "<b>badRecordsPath</b>" Option, it is possible to "<b>Specify</b>" a "<b>Path</b>", where "<b>All</b>" the "<b>Rows</b>", having the "<b>Error Values</b>" will be "<b>Stored</b>", along with the "<b>Reason</b>" for the "<b>Error Values</b>" in forms of "<b>Exception Message</b>". "<b>All</b>" the "<b>Rows</b>", having the "<b>Error Values</b>" will be "<b>Dropped Off</b>" from the "<b>Created DataFrame</b>".
# MAGIC * Basically, the "<b>badRecordsPath</b>" Option "<b>Redirects</b>" "<b>All</b>" the "<b>Rows</b>" that have the "<b>Error Values</b>" into a "<b>Separate File</b>", and, the "<b>Path</b>" to that "<b>File</b>" can be "<b>Specified</b>" using the "<b>option ()</b>" Method of the "<b>DataFrameReader</b>", while "<b>Reading</b>" a "<b>File</b>".

# COMMAND ----------

# DBTITLE 1,First, Delete the Folder "badRecordsPath"
dbutils.fs.rm("dbfs:/FileStore/tables/retailer/data/badRecordsPath", True)

# COMMAND ----------

# DBTITLE 1,Save Malformed Records in a Path Using "badRecordsPath" Option
# It can be clearly seen that "All" the "Rows", having the "Error Values" are "Dropped Off" from the "Created DataFrame".
df_ReadHouseholDemographicsWithBadRecordsPath = spark.read\
                                                     .option("badRecordsPath", "dbfs:/FileStore/tables/retailer/data/badRecordsPath")\
                                                     .csv(path = "dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat", sep = "|", header = True, schema = householdDemographicsSchema)

display(df_ReadHouseholDemographicsWithBadRecordsPath)

# COMMAND ----------

# DBTITLE 1,Display Bad Records
# Find the "Name" of the "File" Created to "Store" "All" the "Rows", having the "Error Values" from the "CSV File".
dbutils.fs.ls("/FileStore/tables/retailer/data/badRecordsPath/20230630T094600/bad_records/") # Name = "part-00000-cf35f04c-8110-4569-ba9e-4b1448dcdae8"

# Read the "File"
dbutils.fs.head("/FileStore/tables/retailer/data/badRecordsPath/20230630T094600/bad_records/part-00000-cf35f04c-8110-4569-ba9e-4b1448dcdae8")
