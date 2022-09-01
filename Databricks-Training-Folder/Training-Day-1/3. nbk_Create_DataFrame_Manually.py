# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 1
# MAGIC * Topic: How to Create "DataFrames"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create a List
employeeColumns = ["Employee_Id", "First_Name", "Last_Name", "House_No", "Address", "City", "Pincode"]

employeeList = [\
                  (1001, "Oindrila", "CHakraborty", "118/H", "Narikel Danga North Road", "Kolkata", 700011),\
                  (1002, "Soumyajyoti", "Bagchi", "38", "Dhakuria East Road", "Kolkata", 700078),\
                  (1003, "Oishi", "Bhattacharyya", "28B", "M.G Road", "Pune", 411009),\
                  (1004, "Sabarni", "Chakraborty", "109A", "Ramkrishna Road", "Kolkata", 700105)\
               ]

# COMMAND ----------

# DBTITLE 1,Create RDD from List Using "parallelize ()" Function
employeeRdd = spark.sparkContext.parallelize(employeeList)
print(type(employeeRdd))
print(employeeRdd.collect())

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "toDF ()" Method
employeeDf = employeeRdd.toDF()
employeeDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "toDF ()" Method to Provide "Column Names"
employeeColumnNamesDf = employeeRdd.toDF(schema = employeeColumns)
print("Type of 'employeeColumnNamesDf' is : " + str(type(employeeColumnNamesDf)))
employeeColumnNamesDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "toDF ()" Method to Provide the "Schema"
employeeSchema = StructType([\
                             StructField("Employee_Id", IntegerType(), False),
                             StructField("First_Name", StringType(), False),
                             StructField("Last_Name", StringType(), False),
                             StructField("House_No", StringType(), False),
                             StructField("Address", StringType(), False),
                             StructField("City", StringType(), False),
                             StructField("Pincode", IntegerType(), False),
                            ])

employeeSchemaDf = employeeRdd.toDF(schema = employeeSchema)
print("Type of 'employeeSchemaDf' is : " + str(type(employeeSchemaDf)))
employeeSchemaDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "createDataFrame ()" Method
employeeDf = spark.createDataFrame(employeeRdd)
print("Type of 'employeeDf' is : " + str(type(employeeDf)))
employeeDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "createDataFrame ()" Method to Provide "Column Names"
employeeColumnNamesDf = spark.createDataFrame(employeeRdd, schema = employeeColumns)
print("Type of 'employeeColumnNamesDf' is : " + str(type(employeeColumnNamesDf)))
employeeColumnNamesDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from an Existing RDD Using "createDataFrame ()" Method to Provide the "Schema"
employeeSchema = StructType([\
                             StructField("Employee_Id", IntegerType(), False),
                             StructField("First_Name", StringType(), False),
                             StructField("Last_Name", StringType(), False),
                             StructField("House_No", StringType(), False),
                             StructField("Address", StringType(), False),
                             StructField("City", StringType(), False),
                             StructField("Pincode", IntegerType(), False),
                            ])

employeeSchemaDf = spark.createDataFrame(employeeRdd, schema = employeeSchema)
print("Type of 'employeeSchemaDf' is : " + str(type(employeeSchemaDf)))
employeeSchemaDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List Using "createDataFrame ()" Method
employeeDf = spark.createDataFrame(employeeList)
print("Type of 'employeeDf' is : " + str(type(employeeDf)))
employeeDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List Using "createDataFrame ()" Method to Provide "Column Names"
employeeColumnNamesDf = spark.createDataFrame(employeeList, schema = employeeColumns)
print("Type of 'employeeColumnNamesDf' is : " + str(type(employeeColumnNamesDf)))
employeeColumnNamesDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List Using "createDataFrame ()" Method to Provide the "Schema"
employeeSchema = StructType([\
                             StructField("Employee_Id", IntegerType(), False),
                             StructField("First_Name", StringType(), False),
                             StructField("Last_Name", StringType(), False),
                             StructField("House_No", StringType(), False),
                             StructField("Address", StringType(), False),
                             StructField("City", StringType(), False),
                             StructField("Pincode", IntegerType(), False),
                            ])

employeeSchemaDf = spark.createDataFrame(employeeList, schema = employeeSchema)
print("Type of 'employeeSchemaDf' is : " + str(type(employeeSchemaDf)))
employeeSchemaDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List of Rows Using "createDataFrame ()" Method
employeeRow = map(lambda x: Row(*x), employeeList)
print(type(employeeRow))
employeeDf = spark.createDataFrame(employeeRow)
employeeDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List of Rows Using "createDataFrame ()" Method to Provide "Column Names"
employeeRow = map(lambda x: Row(*x), employeeList)
employeeColumnNamesDf = spark.createDataFrame(employeeRow, schema = employeeColumns)
print("Type of 'employeeColumnNamesDf' is : " + str(type(employeeColumnNamesDf)))
employeeColumnNamesDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a List of Rows Using "createDataFrame ()" Method to Provide the "Schema"
employeeRow = map(lambda x: Row(*x), employeeList)
employeeSchema = StructType([\
                             StructField("Employee_Id", IntegerType(), False),
                             StructField("First_Name", StringType(), False),
                             StructField("Last_Name", StringType(), False),
                             StructField("House_No", StringType(), False),
                             StructField("Address", StringType(), False),
                             StructField("City", StringType(), False),
                             StructField("Pincode", IntegerType(), False),
                            ])

employeeSchemaDf = spark.createDataFrame(employeeRow, schema = employeeSchema)
print("Type of 'employeeSchemaDf' is : " + str(type(employeeSchemaDf)))
employeeSchemaDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from CSV File With "|" as Delimiter Using "csv ()" Method
customerCsvDf = spark.read.\
                          options(\
                                  sep = "|",\
                                  header = True,\
                                  inferSchema = True\
                                 ).\
                          csv("/FileStore/tables/retailer/data/customer.dat")

print("Type of 'customerCsvDf' is : " + str(type(customerCsvDf)))
customerCsvDf.printSchema()

# COMMAND ----------

# DBTITLE 1,Create DataFrame from Text File Using "text ()" Method
textFileDf = spark.read.text("/FileStore/tables/DCAD/Oindrila_File_txt.txt")
print("Type of 'textFileDf' is : " + str(type(textFileDf)))
textFileDf.printSchema()
display(textFileDf)

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a Single-Line JSON File Using "json ()" Method
singleLineJsonDf = spark.read.json("dbfs:/FileStore/tables/retailer/data/single_line.json")
print("Type of 'singleLineJsonDf' is : " + str(type(singleLineJsonDf)))
singleLineJsonDf.printSchema()
display(singleLineJsonDf)

# COMMAND ----------

# DBTITLE 1,Create DataFrame from a Multi-Line JSON File Using "json ()" Method
multiLineJsonDf = spark.read.option("multiLine", "true").json("dbfs:/FileStore/tables/retailer/data/multi_line.json")
print("Type of 'multiLineJsonDf' is : " + str(type(multiLineJsonDf)))
multiLineJsonDf.printSchema()
display(multiLineJsonDf)
