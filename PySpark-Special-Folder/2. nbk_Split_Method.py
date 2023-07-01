# Databricks notebook source
# DBTITLE 1,Define a "JSON String"
employeeJsonString = """
[
    {
        "firstName": "Oindrila",
        "lastName": "Chakraborty",
        "company": "Aryasoft Technologies, Cognizant Technology Solutions, KPMG, Digital Avenues Limited, PwC, Cognizant Technology Solutions, LTIMindtree",
        "expInCompany": "1.8, 2.11, 0.6, 0.8, 2.10, 0.11, 1.3"
    },
    {
        "firstName": "Soumyajyoti",
        "lastName": "Bagchi",
        "company": "Tata Consultancy Services, Cognizant Technology Solutions, Accenture India Pvt Ltd",
        "expInCompany": "6.7, 6.2, 1.1"
    },
    {
        "firstName": "Ayan",
        "lastName": "Dutta",
        "company": "Tata Consultancy Services, Cognizant Technology Solutions, Eli Lily",
        "expInCompany": "4.3, 3.2, 1.4"
    },
    {
        "firstName": "Debarshi",
        "lastName": "Das",
        "company": "Tata Consultancy Services, Cognizant Technology Solutions, KPMG, Eli Lily",
        "expInCompany": "3.8, 4.3, 0.9, 2.1"
    }
]
"""

# COMMAND ----------

# DBTITLE 1,"Load" the "JSON String" to a "DataFrame"
import json

# "Create" a "List" of "Objects" from the "JSON String" using the "json.loads ()" Method 
employeeList = json.loads(employeeJsonString)

# "Create" a "DataFrame" from a "List" of "Objects"
dfEmployee = spark.createDataFrame(employeeList)

# Display the Content of the Created DataFrame from a "JSON String"
display(dfEmployee)

# Print the "Schema" of the Created DataFrame from a "JSON String"
dfEmployee.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # "split ()" Method
# MAGIC * It is possible to "<b>Create</b>" an "<b>Array</b>" from a "<b>Column</b>" of "<b>String Type</b>", after "<b>Splitting</b>" the "<b>Value</b>" of that "<b>Column</b>" by a "<b>Given Delimiter</b>" using the "<b>split ()</b>" Method form the "<b>pyspark.sql.functions</b>" Package.

# COMMAND ----------

# DBTITLE 1,Create "New Columns" of "Array Type" Using the "split ()" Method
from pyspark.sql.functions import *

# Display the following Columns -
# 1. "firstName" of "String Data Type",
# 2. "lastName" of "String Data Type", 
# 3. "arrayOfCompanies" of "Array Data Type"
# 4. "arrayOfExperiences" of "Array Data Type"

dfEmployeeWithArrayColumns = dfEmployee.withColumn("arrayOfCompanies", split(col("company"), ","))\
                                       .withColumn("arrayOfExperiences", split(col("expInCompany"), ","))\
                                       .drop("company", "expInCompany")
display(dfEmployeeWithArrayColumns)

# Print the "Schema" of the Created DataFrame
dfEmployeeWithArrayColumns.printSchema()
