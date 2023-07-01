# Databricks notebook source
# MAGIC %md
# MAGIC # Project Single, or, Multiple Columns of a DataFrame in Databricks
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Select a Single or Multiple Columns from that DataFrame in Different Scenarios.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .csv(path = "dbfs:/FileStore/tables/retailer/data/customer.csv", header = True, inferSchema = True)
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Select a Single Column from a DataFrame Using String-Representation Column Name
display(df_ReadCustomerFileUsingCsv.select("c_current_cdemo_sk"))

# COMMAND ----------

# DBTITLE 1,Select a Single Column from a DataFrame Using Column Object Reference
from pyspark.sql.functions import col

display(\
         df_ReadCustomerFileUsingCsv.select(col("c_current_cdemo_sk"))\
       )

# COMMAND ----------

# DBTITLE 1,Select a Single Column from a DataFrame Using the Nested Columns of DataFrame Format - "DataFrame.Column Name"
display(\
         df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv.c_current_cdemo_sk)\
       )

# COMMAND ----------

# DBTITLE 1,Select a Single Column from the List of Columns of a DataFrame Using "DataFrame[Column Name]" Format
display(\
         df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv["c_current_cdemo_sk"])\
       )

# COMMAND ----------

# DBTITLE 1,Select Multiple Columns from a DataFrame Using String-Representation Column Names
display(df_ReadCustomerFileUsingCsv.select("c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk"))

# COMMAND ----------

# DBTITLE 1,Select Multiple Columns from a DataFrame Using Column Object Reference
from pyspark.sql.functions import col, column

display(\
        df_ReadCustomerFileUsingCsv.select(col("c_customer_sk"),\
                                           column("c_customer_id"),\
                                           col("c_current_cdemo_sk"),\
                                           column("c_current_hdemo_sk"))\
       )

# COMMAND ----------

# DBTITLE 1,Select Multiple Columns from a DataFrame Using the Nested Columns of DataFrame Format - "DataFrame.Column Name"
display(\
        df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv.c_customer_sk,\
                                           df_ReadCustomerFileUsingCsv.c_customer_id,\
                                           df_ReadCustomerFileUsingCsv.c_current_cdemo_sk,\
                                           df_ReadCustomerFileUsingCsv.c_current_hdemo_sk)\
       )

# COMMAND ----------

# DBTITLE 1,Select Multiple Columns from the List of Columns of a DataFrame Using "DataFrame[Column Name]" Format
display(\
        df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv["c_customer_sk"],\
                                           df_ReadCustomerFileUsingCsv["c_customer_id"],\
                                           df_ReadCustomerFileUsingCsv["c_current_cdemo_sk"],\
                                           df_ReadCustomerFileUsingCsv["c_current_hdemo_sk"])\
       )

# COMMAND ----------

# DBTITLE 1,Select All Columns Using "columns" List Object of "DataFrame" Class
display(df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv.columns))

# COMMAND ----------

# DBTITLE 1,Select All Columns Using "*" in "select()" Function
display(df_ReadCustomerFileUsingCsv.select("*"))

# COMMAND ----------

# DBTITLE 1,Select All Columns By Creating a List Using "ForEach" Loop to Iterate All Columns from "columns" List Object of "DataFrame" Class
allColumns = [] #Create an empty list
for column in df_ReadCustomerFileUsingCsv.columns: #Loop over an iterable
  allColumns.append(column) #Append each element to the end of the list

display(\
         df_ReadCustomerFileUsingCsv.select(allColumns)
       )

# COMMAND ----------

# DBTITLE 1,Select All Columns By Creating a List Using "List Comprehension" to Iterate All Columns from "columns" List Object of "DataFrame" Class
allColumns = [column for column in df_ReadCustomerFileUsingCsv.columns]

display(\
         df_ReadCustomerFileUsingCsv.select(allColumns)\
       )

# COMMAND ----------

# DBTITLE 1,Select All the Rows of a Column, Having a Particular Column Index from a DataFrame
# Select All the Rows of the Column Having Index "8" from the "df_ReadCustomerFileUsingCsv" DataFrame
display(df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv.columns[8]))

# COMMAND ----------

# DBTITLE 1,Column Index of a DataFrame Can't Be Anything But "Integer"
# display(df_ReadCustomerFileUsingCsv.select(df_ReadCustomerFileUsingCsv.columns['c_last_name']))

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
ddl_IncomeBandSchema = "ib_lower_band_sk long, ib_lower_bound int, ib_upper_bound int"

df_ReadIncomeBandUsingCsv = spark.read\
                                 .option("header", "true")\
                                 .option("sep", "|")\
                                 .schema(ddl_IncomeBandSchema)\
                                 .csv("dbfs:/FileStore/tables/retailer/data/income_band.dat")
display(df_ReadIncomeBandUsingCsv)

# COMMAND ----------

# DBTITLE 1,Use "case()" and "otherwise()" Functions in "select()" Function to Add New Projection Columns
from pyspark.sql.functions import col, when

display(\
         df_ReadIncomeBandUsingCsv\
                                .select("*", when(col("ib_upper_bound") < 60000, "First")\
                                             .when((col("ib_upper_bound") >= 60000) & (col("ib_upper_bound") < 120000), "Second")\
                                             .when((col("ib_upper_bound") >= 120000) & (col("ib_upper_bound") < 200000), "Third")\
                                             .otherwise("NA").alias("IncomeGroup"))\
       )

# COMMAND ----------

# DBTITLE 1,Use "lit()" Function in "select()" Function to Add New Projection Columns
from pyspark.sql.functions import lit

display(\
        df_ReadIncomeBandUsingCsv\
                                .select("ib_lower_band_sk", "ib_lower_bound", "ib_upper_bound", lit("newString").alias("col_new_string"), lit(3).alias("col_3"))\
       )

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .option("inferSchema", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

df_ReadCustomerFileUsingCsv.printSchema()

# COMMAND ----------

# DBTITLE 1,Change Datatype of Columns Using "select()" Function Along With "cast()" Function
from pyspark.sql.functions import col
from pyspark.sql.types import *

display(\
            df_ReadCustomerFileUsingCsv\
                                        .select(col("c_first_shipto_date_sk").cast(StringType()), col("c_first_name").cast("integer"))
       )

# COMMAND ----------

# DBTITLE 1,Change Datatype of Columns Using "select ()" Function Along With "expr ()" Functions
from pyspark.sql.functions import expr

df_ChangeColumnDataTypeUsingSelectExpr = df_ReadCustomerFileUsingCsv\
                                                    .select(expr("cast(c_first_shipto_date_sk as string)"), expr("cast(c_first_name as integer)"))
display(df_ChangeColumnDataTypeUsingSelectExpr)

# COMMAND ----------

# DBTITLE 1,Change Datatype of Columns Using "selectExpr()" Function
df_ChangeColumnDataTypeUsingSelectExpr = df_ReadCustomerFileUsingCsv\
                                                    .selectExpr("cast(c_first_shipto_date_sk as string)", "cast(c_first_name as integer)")
display(df_ChangeColumnDataTypeUsingSelectExpr)
