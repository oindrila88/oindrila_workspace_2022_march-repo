# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFrames From JSON File
# MAGIC * Topic: Read JSON File and Load the Data into a DataFrame with Different Options
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a Single-Line JSON File Using "json" method of "DataFrameReader" and Create a DataFrame
df_ReadSingleLineJson = spark.read.json(path = "dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJson)

# COMMAND ----------

# DBTITLE 1,Read a Single-Line JSON File Using "format" method of "DataFrameReader" and Create a DataFrame
df_ReadSingleLineJsonUsingFormat = spark.read.format("json").load("dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJsonUsingFormat)
df_ReadSingleLineJsonUsingFormat.printSchema()

# COMMAND ----------

# DBTITLE 1,Create JSON Schema Using DDL-Formatted String and a DataFrame Using that Schema
ddl_SingleLineJsonSchema = "address struct<city: string, country: string, state: string>, birthday date, email string, first_name string,id bigint, last_name string, skills array<string>"

df_ReadSingleLineJsonWithSchema = spark.read.json(path = "dbfs:/FileStore/tables/retailer/data/single_line.json", schema = ddl_SingleLineJsonSchema)
display(df_ReadSingleLineJsonWithSchema)

# COMMAND ----------

# DBTITLE 1,Create JSON Schema Using Struct Type and a DataFrame Using that Schema
from pyspark.sql.types import *

singleLineJsonSchema = StructType([
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("birthday", DateType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("id", LongType(), True),
    StructField("last_name", StringType(), True),
    StructField("skills", ArrayType(StringType()), True)
])

df_ReadSingleLineJsonWithSchema = spark.read.json(path = "dbfs:/FileStore/tables/retailer/data/single_line.json", schema = singleLineJsonSchema)
display(df_ReadSingleLineJsonWithSchema)
df_ReadSingleLineJsonWithSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,Use "dateFormat" Property of "option" Method of DataFrameReader to Provide Custom Date Format
df_ReadSingleLineJsonWithCustomDate = spark.read.json(path = "dbfs:/FileStore/tables/retailer/data/single_line.json", schema = ddl_SingleLineJsonSchema, dateFormat = "dd.MM.yyyy")
display(df_ReadSingleLineJsonWithCustomDate)

# COMMAND ----------

# DBTITLE 1,Read a Multi-Line JSON File Using "json" Method of "DataFrameReader" and "multiLine" Property of "Option" Method of DataFrameReader and Create a DataFrame
df_ReadMultiLineJson = spark.read.json(path = "dbfs:/FileStore/tables/retailer/data/multi_line.json", multiLine = True)
display(df_ReadMultiLineJson)
