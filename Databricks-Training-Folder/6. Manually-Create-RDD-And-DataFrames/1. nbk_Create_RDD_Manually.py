# Databricks notebook source
# MAGIC %md
# MAGIC # Manually Create RDD
# MAGIC * Topic: How to Create "Resilient Distributed Datasets" (RDD)?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Resilient Distributed Dataset" (RDD)
# MAGIC * A "<b>Resilient Distributed Dataset</b>" (RDD) is the "Basic Abstraction" in "Spark" that "Represents" an "Immutable", "Partitioned Collection" of "Language-Native Elements", like "Lists" in "Python", that "Runs" and being "Operated" on "Multiple Nodes" to do the "Parallel Processing" on a "Cluster".

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create a List
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
print(employeeRdd)
print(employeeRdd.collect())
display(employeeRdd.collect())

# COMMAND ----------

# DBTITLE 1,Create RDD from List Using "parallelize ()" Function Having "Partition Numbers"
employeeWithPartitionRdd = spark.sparkContext.parallelize(employeeList, 6)
print("Number of Partitions: " + str(employeeWithPartitionRdd.getNumPartitions()))

# COMMAND ----------

# DBTITLE 1,Create Empty RDD Using "parallelize ()" Function
emptyRddParallelize = spark.sparkContext.parallelize([])
print("Is 'emptyRddParallelize' Empty: " + str(emptyRddParallelize.isEmpty()))
print("Is 'employeeWithPartitionRdd' Empty: " + str(employeeWithPartitionRdd.isEmpty()))

# COMMAND ----------

# DBTITLE 1,Create Empty RDD Using "emptyRDD()" Function
emptyRdd = spark.sparkContext.emptyRDD()
print("Is 'emptyRdd' Empty: " + str(emptyRdd.isEmpty()))

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/DCAD/Oindrila_File_txt.txt")

# COMMAND ----------

# DBTITLE 1,Create an RDD from a text file Using "textFile"
textFileRdd = spark.sparkContext.textFile("/FileStore/tables/DCAD/Oindrila_File_txt.txt")
for text in textFileRdd.collect():
  print(text)
print(textFileRdd.collect())

# COMMAND ----------

# DBTITLE 1,Create an RDD from a text file Using "wholeTextFiles"
textFilesRdd = spark.sparkContext.wholeTextFiles("/FileStore/tables/DCAD/Oindrila_File_txt.txt")
print(textFilesRdd.collect())

# COMMAND ----------

# DBTITLE 1,Create an RDD from Another Existing RDD
employeeFilterRdd = employeeRdd.filter(lambda row: row[5] == 'Kolkata')
for employee in employeeFilterRdd.collect():
  print(employee)
print(employeeFilterRdd.collect())

# COMMAND ----------

# DBTITLE 1,Create an RDD from Existing DataFrames and DataSet
from pyspark.sql.types import *

employeeSchema = StructType([\
                             StructField("Employee_Id", IntegerType(), False),
                             StructField("First_Name", StringType(), False),
                             StructField("Last_Name", StringType(), False),
                             StructField("House_No", StringType(), False),
                             StructField("Address", StringType(), False),
                             StructField("City", StringType(), False),
                             StructField("Pincode", IntegerType(), False),
                            ])

dfEmployee = spark.createDataFrame(employeeList, schema = employeeSchema)
print("Data Type of 'dfEmployee' is:" + str(type(dfEmployee)))
dfEmployee.printSchema()

rddEmployee = dfEmployee.rdd
print("Data Type of 'rddEmployee' is:" + str(type(rddEmployee)))
