# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFrames From CSV File
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame with Different Options
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read.csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "format" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingFormat = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingFormat)

# COMMAND ----------

# DBTITLE 1,Display the Contents of a DataFrame Along With Header Column Names Using "option"
df_ReadCustomerFileWithHeader = spark.read\
                                     .option("header", "true")\
                                     .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Display the Contents of a ".dat" File Along With Header Column Names Using "csv" Method of "DataFrameReader"
df_ReadCustomerDatFileWithHeader = spark.read\
                                        .option("header", "true")\
                                        .option("sep", "|")\
                                        .format("csv")\
                                        .load("dbfs:/FileStore/tables/retailer/data/customer.dat")
      
display(df_ReadCustomerDatFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Display All the Column Names of a DataFrame Using "columns" Property of DataFrame
df_ReadCustomerFileWithHeader.columns

# COMMAND ----------

# DBTITLE 1,Display the Number of Columns Present in a DataFrame
len(df_ReadCustomerFileWithHeader.columns)

# COMMAND ----------

# DBTITLE 1,Select Specific Columns from the DataFrame "df_ReadCustomerDatFileWithHeader" and Create a New DataFrame that Contains Only the Selected Columns
df_CustomerWithBDay = df_ReadCustomerDatFileWithHeader.select(
                                                              "c_customer_id",
                                                              "c_first_name",
                                                              "c_last_name",
                                                              "c_birth_year",
                                                              "c_birth_month",
                                                              "c_birth_day"
                                                            )
display(df_CustomerWithBDay)

# COMMAND ----------

# DBTITLE 1,Display the Contents of a DataFrame Using "show" Method of DataFrame
df_CustomerWithBDay.show()

# COMMAND ----------

# DBTITLE 1,Refer to Columns Using Column Objects of "PySpark.Sql.Functions.Col/Column"
from pyspark.sql.functions import col, column

df_CustomerWithBDayUsingColObj = df_ReadCustomerDatFileWithHeader.select(
                                                                            col('c_customer_id'),
                                                                            column('c_first_name'),
                                                                            col("c_birth_day"),
                                                                            "c_birth_year"
                                                                        )
display(df_CustomerWithBDayUsingColObj)

# COMMAND ----------

# DBTITLE 1,View the Schema of a DataFrame Using "printSchema()" Method of DataFrame
df_ReadCustomerDatFileWithHeader.printSchema()

# COMMAND ----------

# DBTITLE 1,View the Schema of a DataFrame Using 'schema' Property of DataFrame
# The "schema" Property of the "DataFrame" returns a "StructType" Object.

df_ReadCustomerDatFileWithHeader.schema

# COMMAND ----------

# DBTITLE 1,Display All the Column Names of a DataFrame Using the "fieldNames ()" Method of the "schema" Property of DataFrame
# The "fieldNames ()" Method of the "schema" Property of the "DataFrame" returns "All" the "Column Names" of a "DataFrame" as a "List" Object.

df_ReadCustomerDatFileWithHeader.schema.fieldNames()

# COMMAND ----------

# DBTITLE 1,Check If a Particular Column, e.g., "c_email_address" Exists in the DataFrame Using the "columns" Property of DataFrame
print(df_ReadCustomerDatFileWithHeader.columns.count("c_email_address") > 0)

# COMMAND ----------

# DBTITLE 1,Check If a Particular Column, e.g., "c_email_address" Exists in the DataFrame Using the "fieldNames ()" Method of the "schema" Property of the "DataFrame"
print(df_ReadCustomerDatFileWithHeader.schema.fieldNames().count("c_email_address") > 0)

# COMMAND ----------

# DBTITLE 1,Let Apache Spark Infer the Data Type of Each Column Based On Its Value in the DataFrame
df_ReadCustomerWithInferSchema = spark.read\
                                      .option("header", "true")\
                                      .option("inferSchema", "true")\
                                      .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
df_ReadCustomerWithInferSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,"Read" the CSV File and "Check" the "Data Type" of the Column "ca_zip"
df_ReadCustomerAddressInferSchema = spark.read\
                                         .option("header", "true")\
                                         .option("sep", "|")\
                                         .option("inferSchema", "true")\
                                         .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
display(df_ReadCustomerAddressInferSchema)

df_ReadCustomerAddressInferSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,Problem with Inferred Schemas - String Being Incorrectly Set as Double
df_ReadCustomerAddressInferSchema = spark.read\
                                         .option("header", "true")\
                                         .option("sep", "|")\
                                         .option("inferSchema", "true")\
                                         .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

df_ReadCustomerAddressInferSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame Schema Using "Data Definition Language-Formatted String" and "schema" Method of the "DataFrameReader" Object
ddl_CustomerAddressSchema = "ca_address_sk long, ca_address_id string, ca_street_number string, ca_street_name string, ca_street_type string, " +\
 "ca_suite_number string, ca_city string, ca_county string, ca_state string, ca_zip string, ca_country string, ca_gmt_offset decimal(5, 2), " +\
"ca_location_type string"

df_ReadCustomerAddressWithDdlSchema = spark.read\
                                           .option("header", "true")\
                                           .option("sep", "|")\
                                           .schema(ddl_CustomerAddressSchema)\
                                           .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

df_ReadCustomerAddressWithDdlSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame Schema Using "Structure Schema" and "schema" Method of the "DataFrameReader" Object
from pyspark.sql.types import *

struct_CustomerAddressSchema = StructType(\
                                             [\
                                                  StructField("ca_address_sk", LongType(), True),\
                                                  StructField("ca_address_id", StringType(), True),\
                                                  StructField("ca_street_number", StringType(), True),\
                                                  StructField("ca_street_name", StringType(), True),\
                                                  StructField("ca_street_type", StringType(), True),\
                                                  StructField("ca_suite_number", StringType(), True),\
                                                  StructField("ca_city", StringType(), True),\
                                                  StructField("ca_county", StringType(), True),\
                                                  StructField("ca_state", StringType(), True),\
                                                  StructField("ca_zip", StringType(), True),\
                                                  StructField("ca_country", StringType(), True),\
                                                  StructField("ca_gmt_offset", DecimalType(5, 2), True),\
                                                  StructField("ca_location_type", StringType(), True)\
                                             ]\
                                         )

df_ReadCustomerAddressWithStructSchema = spark.read\
                                              .option("header", "true")\
                                              .option("sep", "|")\
                                              .schema(struct_CustomerAddressSchema)\
                                              .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")

df_ReadCustomerAddressWithStructSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,Usage of "options" Method
df_ReadHouseholDemographics = spark.read\
                                   .options(\
                                             header = "true",
                                             sep = "|",
                                             inferSchema = "true"
                                           )\
                                   .csv("dbfs:/FileStore/tables/retailer/data/household_demographics.dat")
df_ReadHouseholDemographics.printSchema()
display(df_ReadHouseholDemographics)

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Bad Data While Reading File
# MAGIC * When reading Data from a File-Based Data Source, Apache Spark SQL faces two typical error cases -
# MAGIC * A. Files may Not be Readable (for instance, the Files could be Missing, Inaccessible or Corrupted).
# MAGIC * B. Even if the Files are Processable, some Records may Not be Parsable, might be due to Syntax Errors and Schema Mismatch.

# COMMAND ----------

# DBTITLE 1,First, Delete the Folder "badRecordsPath"
dbutils.fs.rm("dbfs:/FileStore/tables/retailer/data/badRecordsPath", True)

# COMMAND ----------

# DBTITLE 1,Try to Read a Missing Input File
try:
    df_ReadMissingInputFile = spark.read\
                                   .format("csv")\
                                   .option("header", "true")\
                                   .option("badRecordsPath", "dbfs:/FileStore/tables/retailer/data/badRecordsPath")\
                                   .load("dbfs:/tmp/parentDir/childDir/firstFile.csv")
    display(df_ReadMissingInputFile)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC # Parsing Mode
# MAGIC * When reading Data from a File-Based Data Source with specified Schema, it is possible that the Data in the Files does Not Match the Schema.
# MAGIC * The consequences depend on the Mode that the Parser runs on. To set the “Mode”, the “mode” Option is used.

# COMMAND ----------

# DBTITLE 1,Display the Content of the CSV File "household_demographics_m.dat"
display(\
            spark.read\
                 .option("header", "true")\
                 .option("sep", "|")\
                 .csv("dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat")\
        )

# COMMAND ----------

# DBTITLE 1,Usage of "permissive" Mode Option
# In “Permissive” Mode, “NULLs” are inserted for Fields that could Not be Parsed correctly.
ddl_HouseholDemographicsSchema = "hd_demo_sk long, hd_income_band_sk long, hd_buy_potential long, hd_dep_count integer, hd_vehicle_count integer"

df_ReadHouseholDemographicsWithPermissiveMode = spark.read\
                                                     .option("header", "true")\
                                                     .option("sep", "|")\
                                                     .schema(ddl_HouseholDemographicsSchema)\
                                                     .option("mode", "permissive")\
                                                     .csv("dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat")
display(df_ReadHouseholDemographicsWithPermissiveMode)

# COMMAND ----------

# DBTITLE 1,Usage of "dropmalformed" Mode Option
# In “Dropmalformed” Mode, the lines that contain Fields that could Not be Parsed correctly, are Dropped.
ddl_HouseholDemographicsSchema = "hd_demo_sk long, hd_income_band_sk long, hd_buy_potential long, hd_dep_count integer, hd_vehicle_count integer"

df_ReadHouseholDemographicsWithDropmalformedMode = spark.read\
                                                        .option("header", "true")\
                                                        .option("sep", "|")\
                                                        .schema(ddl_HouseholDemographicsSchema)\
                                                        .option("mode", "dropmalformed")\
                                                        .csv("dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat")
display(df_ReadHouseholDemographicsWithDropmalformedMode)

# COMMAND ----------

# DBTITLE 1,Usage of "failfast" Mode Option
# In “Failfast” Mode, Apache Spark aborts the reading with Exception, if any Malformed Data is found.
ddl_HouseholDemographicsSchema = "hd_demo_sk long, hd_income_band_sk long, hd_buy_potential long, hd_dep_count integer, hd_vehicle_count integer"

df_ReadHouseholDemographicsWithFailfastMode = spark.read\
                                                   .option("header", "true")\
                                                   .option("sep", "|")\
                                                   .schema(ddl_HouseholDemographicsSchema)\
                                                   .option("mode", "failfast")\
                                                   .csv("dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat")
display(df_ReadHouseholDemographicsWithFailfastMode)

# COMMAND ----------

# DBTITLE 1,Save Malformed Records in a Path Using "badRecordsPath" Option
# It is possible to obtain the Exception Records/Files and retrieve the Reason of Exception from the “Exception Logs”, by setting the “data source” Option “badRecordsPath”.
ddl_HouseholDemographicsSchema = "hd_demo_sk long, hd_income_band_sk long, hd_buy_potential long, hd_dep_count integer, hd_vehicle_count integer"

df_ReadHouseholDemographicsWithBadRecordsPath = spark.read\
                                                     .options(
                                                                  header ="true",\
                                                                  sep = "|",\
                                                                  badRecordsPath = "dbfs:/FileStore/tables/retailer/data/badRecordsPath"\
                                                             )\
                                                     .schema(ddl_HouseholDemographicsSchema)\
                                                     .csv("dbfs:/FileStore/tables/retailer/data/household_demographics_m.dat")

display(df_ReadHouseholDemographicsWithBadRecordsPath)

# COMMAND ----------

# DBTITLE 1,Display Bad Records
# MAGIC %fs
# MAGIC head /FileStore/tables/retailer/data/badRecordsPath/20221108T043141/bad_records/part-00000-838af052-7f75-45b3-88a5-0dc1a90aaaa5
