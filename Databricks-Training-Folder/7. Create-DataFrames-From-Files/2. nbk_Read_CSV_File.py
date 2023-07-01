# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFrames From CSV File
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame with Different Options
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read a CSV File Using "csv" method

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read.csv(path = "dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame Along With Header Names
df_ReadCustomerFileWithHeader = spark.read\
                                     .csv(path = "dbfs:/FileStore/tables/retailer/data/customer.csv", header = True)
display(df_ReadCustomerFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Display the Contents of a ".dat" File Along With Header Names Using "csv" Method of "DataFrameReader"
df_ReadCustomerDatFileWithHeader = spark.read\
                                        .csv(path = "dbfs:/FileStore/tables/retailer/data/customer.dat", sep = "|", header= True)
display(df_ReadCustomerDatFileWithHeader)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read a CSV File Using "load" method

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "load" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingFormat = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingFormat)

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "load" method of "DataFrameReader" and Create a DataFrame Along With Header Names
df_ReadCustomerFileWithHeader = spark.read\
                                     .format("csv")\
                                     .option("header", "true")\
                                     .load("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileWithHeader)

# COMMAND ----------

# DBTITLE 1,Display the Contents of a ".dat" File Along With Header Names Using "load" Method of "DataFrameReader"
df_ReadCustomerDatFileWithHeader = spark.read\
                                        .format("csv")\
                                        .option("header", "true")\
                                        .option("sep", "|")\
                                        .load("dbfs:/FileStore/tables/retailer/data/customer.dat")
display(df_ReadCustomerDatFileWithHeader)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using "options" Method with "load ()" Method on a DataFrame

# COMMAND ----------

# DBTITLE 1,Usage of "options ()" Method
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
# MAGIC ## Display the Schema of a DataFrame

# COMMAND ----------

# DBTITLE 1,Display All the Column Names of a DataFrame Using "columns" Property of DataFrame
# The "columns" Property of a "DataFrameObject" Returns a "List" that "Contains" "All the Columns Names" of that "DataFrameObject"
df_ReadCustomerFileWithHeader.columns

# COMMAND ----------

# DBTITLE 1,Display the Number of Columns Present in a DataFrame
len(df_ReadCustomerFileWithHeader.columns)

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

# DBTITLE 1,Display All the Column Names Along With Each Columns' Data Types of a DataFrame Using the "dtypes" Property of DataFrame
# The "dtypes" Property of a "DataFrame" Returns a "List of Tuple", where "Each Tuple" Contains "Two Items" -
# A. Column Name
# B. Data Type of that Column
df_ReadCustomerDatFileWithHeader.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check If a Particular Column Exists in a DataFrame

# COMMAND ----------

# DBTITLE 1,Check If a Particular Column, e.g., "c_email_address" Exists in the DataFrame Using the "columns" Property of DataFrame
print(df_ReadCustomerDatFileWithHeader.columns.count("c_email_address") > 0)

# COMMAND ----------

# DBTITLE 1,Check If a Particular Column, e.g., "c_email_address" Exists in the DataFrame Using the "fieldNames ()" Method of the "schema" Property of the "DataFrame"
print(df_ReadCustomerDatFileWithHeader.schema.fieldNames().count("c_email_address") > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Contents of a DataFrame

# COMMAND ----------

# DBTITLE 1,Display the Contents of a DataFrame Using "show" Method of DataFrame
df_ReadCustomerFileWithHeader.show()

# COMMAND ----------

# DBTITLE 1,Display "All the Rows" of a "DataFrame" using "collect ()" Method of "DataFrame"
# The "collect ()" Method of a "DataFrame" Returns the "List of Row Objects". Each "Row Object" is "Each Row" of the "DataFrame".
df_ReadCustomerFileWithHeader.collect()

# COMMAND ----------

# DBTITLE 1,Display "First Few Rows" of a "DataFrame" using "take ()" Method of "DataFrame"
# The "take ()" Method of a "DataFrame" Takes an Argument of "How Many Top Rows to Display", and, Returns the "List of Row Objects" of the "Desired Numbers". Each "Row Object" is "Each Row" of the "DataFrame".
print("Displaying the Top 4 Rows : ")
print(df_ReadCustomerFileWithHeader.take(4))

# If "No Argument" is Provided to the "take ()" Method, then "TypeError" Exception is Thrown.
print(df_ReadCustomerFileWithHeader.take())

# COMMAND ----------

# DBTITLE 1,Display "First Few Rows" of a "DataFrame" using "head ()" Method of "DataFrame"
# The "head ()" Method of a "DataFrame" Takes an Argument of "How Many Top Rows to Display", and, Returns the "List of Row Objects" of the "Desired Numbers". Each "Row Object" is "Each Row" of the "DataFrame".
print("Displaying the Top 4 Rows : ")
print(df_ReadCustomerFileWithHeader.head(4))

# If "No Argument" is Provided to the "head ()" Method, then "Only" the "Top Row" is Displayed.
print("Displaying the Top Row : ")
print(df_ReadCustomerFileWithHeader.head())

# COMMAND ----------

# DBTITLE 1,Display "Last Few Rows" of a "DataFrame" using "tail ()" Method of "DataFrame"
# The "tail ()" Method of a "DataFrame" Takes an Argument of "How Many Last Rows to Display", and, Returns the "List of Row Objects" of the "Desired Numbers". Each "Row Object" is "Each Row" of the "DataFrame".
print("Displaying the Last 4 Rows : ")
print(df_ReadCustomerFileWithHeader.tail(4))

# If "No Argument" is Provided to the "tail ()" Method, then "TypeError" Exception is Thrown.
print(df_ReadCustomerFileWithHeader.tail())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Structure of a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC * When a "<b>File</b>" is "<b>Read</b>" by "<b>Apache Spark</b>", even if that "<b>File</b>" contains "<b>Many Columns</b>" of "<b>Different Data Types</b>", like - "<b>Float</b>", "<b>Integer</b>", or, "<b>Boolean</b>", still "<b>Apache Spark</b>" will "<b>Automatically Read Each Column</b>" of that "<b>File</b>" as "<b>String</b>".

# COMMAND ----------

# DBTITLE 1,"Apache Spark" Considers the "Data Type" of "All" the "Columns" of a "DataFrame" as "String"
df_ReadCustomerWithInferSchema = spark.read\
                                      .csv(path = "dbfs:/FileStore/tables/retailer/data/customer.csv", header = True)
df_ReadCustomerWithInferSchema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inferring the Data Type of a DataFrame

# COMMAND ----------

# DBTITLE 1,Let Apache Spark Infer the Data Type of Each Column Based On Its Value in the DataFrame
# By Passing the "inferSchema" Option to the "csv ()" Method, "Apache Spark" can "Find" the "Proper Data Type" for "Each Column" of the "DataFrame" Based on the Value of "Each Column".
df_ReadCustomerWithInferSchema = spark.read\
                                      .csv(path = "dbfs:/FileStore/tables/retailer/data/customer.csv", header = True, inferSchema = True)
df_ReadCustomerWithInferSchema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inferring the Data Type of a DataFrame Does Not Always Work

# COMMAND ----------

# DBTITLE 1,Problem with Inferred Schemas - String / Integer Being Incorrectly Set as Double
df_ReadCustomerAddressInferSchema = spark.read\
                                         .csv(path = "dbfs:/FileStore/tables/retailer/data/customer_address.dat", sep = "|", header = True, inferSchema = True)
display(df_ReadCustomerAddressInferSchema)

# The following is clearly obvious -
# 1. The "Data Type" of the Column "ca_street_number" is Displayed as "double", where  the Values in the Column "ca_street_number" are all "String".
# 2. The "Data Type" of the Column "ca_zip" is Displayed as "double", where  the Values in the Column "ca_zip" are all "String".
# 3. The "Data Type" of the Column "ca_gmt_offset" is Displayed as "double", where  the Values in the Column "ca_gmt_offset" are all "String".
df_ReadCustomerAddressInferSchema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Structure of a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Structure of a DataFrame Using "DDL-Formatted String"

# COMMAND ----------

# DBTITLE 1,Create DataFrame Schema Using "Data Definition Language-Formatted String" and "schema" Method of the "DataFrameReader" Object
ddl_CustomerAddressSchema = "ca_address_sk long, ca_address_id string, ca_street_number string, ca_street_name string, ca_street_type string, ca_suite_number string, ca_city string, ca_county string, ca_state string, ca_zip string, ca_country string, ca_gmt_offset string, ca_location_type string"

df_ReadCustomerAddressWithDdlSchema = spark.read\
                                           .csv(path = "dbfs:/FileStore/tables/retailer/data/customer_address.dat", sep = "|", header= True, schema = ddl_CustomerAddressSchema)

display(df_ReadCustomerAddressWithDdlSchema)

df_ReadCustomerAddressWithDdlSchema.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Structure of a DataFrame Using "StructType"

# COMMAND ----------

# MAGIC %md
# MAGIC * A "<b>Database Table</b>" contains "<b>Multiple Fields</b>". So, while "<b>Defining</b>" the "<b>Structure</b>" of a "<b>Database Table</b>", the following needs to be defined -
# MAGIC <br>1. <b>Number</b> of <b>Columns</b>
# MAGIC <br>2. <b>Name</b> of <b>Each Column</b>
# MAGIC <br>3. <b>Data Type</b> of <b>Each Column</b>
# MAGIC <p></p>
# MAGIC * For a "<b>Structured Data</b>", "<b>All</b>" the "<b>Rows</b>" have the "<b>Same Structure</b>".
# MAGIC * In "<b>Apache Spark</b>", the "<b>Structure</b>" of the "<b>Row</b>" is defined using the "<b>StructType</b>" Class.
# MAGIC * The "<b>Constructor</b>" of the "<b>StructType</b>" Class takes a "<b>Collection</b>" of "<b>StructField Class Objects</b>" as the "<b>Argument</b>", which describes the "<b>Metadata</b>" of "<b>Each Column</b>" in the "<b>Row</b>".
# MAGIC * The "<b>Constructor</b>" of the "<b>StructField</b>" Class takes the following "<b>Inputs</b>" -
# MAGIC <br>1. <b>Name</b> of the <b>Column</b>
# MAGIC <br>2. <b>Data Type</b> of the <b>Column</b>
# MAGIC <br>3. A "<b>Boolean Value</b>" defining <b>Whether</b> the <b>Column</b> is "<b>Nullable</b>".

# COMMAND ----------

# DBTITLE 1,Create DataFrame Schema Using "Structure Schema" and "schema" Method of the "DataFrameReader" Object
from pyspark.sql.types import *

struct_CustomerAddressSchema = StructType([\
                                            StructField("ca_address_sk", LongType(), True),
                                            StructField("ca_address_id", StringType(), True),
                                            StructField("ca_street_number", StringType(), True),
                                            StructField("ca_street_name", StringType(), True),
                                            StructField("ca_street_type", StringType(), True),
                                            StructField("ca_suite_number", StringType(), True),
                                            StructField("ca_city", StringType(), True),
                                            StructField("ca_county", StringType(), True),
                                            StructField("ca_state", StringType(), True),
                                            StructField("ca_zip", StringType(), True),
                                            StructField("ca_country", StringType(), True),
                                            StructField("ca_gmt_offset", DecimalType(5, 2), True),
                                            StructField("ca_location_type", StringType(), True)
                                         ])

df_ReadCustomerAddressWithStructSchema = spark.read\
                                              .csv(path = "dbfs:/FileStore/tables/retailer/data/customer_address.dat", sep = "|", header = True, schema = struct_CustomerAddressSchema)

df_ReadCustomerAddressWithStructSchema.printSchema()
