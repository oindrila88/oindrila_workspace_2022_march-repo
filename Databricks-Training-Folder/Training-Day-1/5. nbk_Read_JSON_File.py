# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 1
# MAGIC * Topic: Read JSON File and Load the Data into a DataFrame with Different Options
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a Single-Line JSON File Using "json" method of "DataFrameReader" and Create a DataFrame
df_ReadSingleLineJson = spark.read.json("dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJson)

# COMMAND ----------

# DBTITLE 1,Read a Single-Line JSON File Using "format" method of "DataFrameReader" and Create a DataFrame
df_ReadSingleLineJsonUsingFormat = spark.read.format("json").load("dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJsonUsingFormat)
df_ReadSingleLineJsonUsingFormat.printSchema()

# COMMAND ----------

# DBTITLE 1,Create JSON Schema Using DDL-Formatted String and a DataFrame Using that Schema
ddl_SingleLineJsonSchema = "address struct<city: string, country: string, state: string>," +\
                        "birthday date," +\
                        "email string," +\
                        "first_name string," +\
                        "id bigint," +\
                        "last_name string," +\
                        "skills array<string>"

df_ReadSingleLineJsonWithSchema = spark.read.schema(ddl_SingleLineJsonSchema).json("dbfs:/FileStore/tables/retailer/data/single_line.json")
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

df_ReadSingleLineJsonWithSchema = spark.read.schema(singleLineJsonSchema).json("dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJsonWithSchema)

# COMMAND ----------

# DBTITLE 1,Use "dateFormat" Property of "option" Method of DataFrameReader to Provide Custom Date Format
df_ReadSingleLineJsonWithCustomDate = spark.read.schema(ddl_SingleLineJsonSchema).option("dateFormat", "dd.MM.yyyy").json("dbfs:/FileStore/tables/retailer/data/single_line.json")
display(df_ReadSingleLineJsonWithCustomDate)

# COMMAND ----------

# DBTITLE 1,Read a Multi-Line JSON File Using "json" Method of "DataFrameReader" and "multiLine" Property of "Option" Method of DataFrameReader and Create a DataFrame
df_ReadMultiLineJson = spark.read.option("multiLine", "true").json("dbfs:/FileStore/tables/retailer/data/multi_line.json")
display(df_ReadMultiLineJson)

# COMMAND ----------

# DBTITLE 1,Read "Array" Element from the Multi-Line JSON File Using the "explode ()" Function
# "explode ()" method returns a new row for each element in the given array. This method uses the default column name "col" for elements in the array, unless specified otherwise.
from pyspark.sql.functions import expr

df_ReadArrayInJson = df_ReadMultiLineJson.select(\
                                                     "id",\
                                                     "first_name",\
                                                     "last_name",\
                                                     "email",\
                                                     "address.city",\
                                                     "address.country",\
                                                     "address.state",\
                                                     expr("explode(skills)").alias("skill")\
                                                )

display(df_ReadArrayInJson)
