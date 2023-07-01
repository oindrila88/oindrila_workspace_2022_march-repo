# Databricks notebook source
# DBTITLE 1,Define a "JSON String"
employeeJsonString = """
[
    {
        "firstName": "Oindrila",
        "lastName": "Chakraborty",
        "company": [
            "Aryasoft Technologies",
            "Cognizant Technology Solutions",
            "KPMG",
            "Digital Avenues Limited",
            "PwC",
            "Cognizant Technology Solutions",
            "LTIMindtree"
        ],
        "expInCompany": [
            1.8,
            2.11,
            0.6,
            0.8,
            2.10,
            0.11,
            1.3
        ]
    },
    {
        "firstName": "Soumyajyoti",
        "lastName": "Bagchi",
        "company": [
            "Tata Consultancy Services",
            "Cognizant Technology Solutions",
            "Accenture India Pvt Ltd"
        ],
        "expInCompany": [
            6.7,
            6.2,
            1.1
        ]
    },
    {
        "firstName": "Ayan",
        "lastName": "Dutta",
        "company": [
            "Tata Consultancy Services",
            "Cognizant Technology Solutions",
            "Eli Lily"
        ],
        "expInCompany": [
            4.3,
            3.2,
            1.4
        ]
    },
    {
        "firstName": "Debarshi",
        "lastName": "Das",
        "company": [
            "Tata Consultancy Services",
            "Cognizant Technology Solutions",
            "KPMG",
            "Eli Lily"
        ],
        "expInCompany": [
            3.8,
            4.3,
            0.9,
            2.1
        ]
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

# In the "Schema", it can be seen that the "Data Type" of the following "Columns" are provided as "Array" by "Apache Spark", even though the "Schema" of the "JSON String" was "Not Provided" while "Creating" the "DataFrame" form the "JSON String" -
# 1. "company" - Provided as "Array of String",
# 2. "expInCompany" - Provided as "Array of Double"
# This happened because, "Based On" the "Type" of the "Data" that is "Passed" to the "createDataFrme ()" Function, it will try to "Infer" the "Schema", or, the "Data Type" of "Each" of the "Columns" "Automatically".

# COMMAND ----------

# DBTITLE 1,Create the "Schema" Using the "StructType" to "Explicitly Mention" the "Data Type" of a "Column" as "Array Type"
# It is possible to "Create" the "Schema" using the "StructType" to "Explicitly Mention" the "Data Type" of a "Column" as "Array Type".
from pyspark.sql.types import *

# Inside the "Constructor" of "ArrayType", the "Data Type" of the "Data" that the "Array" will "Contain" needs to be  "Passed".
personSchema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("company", ArrayType(StringType()), True),
    StructField("expInCompany", ArrayType(FloatType()), True)
])

# COMMAND ----------

# DBTITLE 1,"Load" the "JSON String" to a "DataFrame" Using the "Created Schema" Containing "Array Type Columns"
import json

# "Create" a "List" of "Objects" from the "JSON String" using the "json.loads ()" Method 
employeeList = json.loads(employeeJsonString)

# "Create" a "DataFrame" from a "List" of "Objects" by "Passing" the "Created Schema" Containing "Array Type Columns"
dfEmployee = spark.createDataFrame(employeeList, schema = personSchema)

# Display the Content of the Created DataFrame from a "JSON String"
display(dfEmployee)

# Print the "Schema" of the Created DataFrame from a "JSON String"
dfEmployee.printSchema()

# COMMAND ----------

# DBTITLE 1,"Access" "Every Element" of an "Array Type Column" by using the "Indexes"
# It is possible to "Access" "Every Element" of an "Array Type Column" by using the "Indexes".

# Create a "New Column" that should have the "Second Element" of the "Array Columns", i.e, "company", and, "expInCompany" in "Each Row" of the "DataFrame".
from pyspark.sql.functions import *

dfEmployeeWithSecondArrayElement = dfEmployee.withColumn("secondCompanyName", col("company")[1])\
                                             .withColumn("expInSecondCompany", col("expInCompany")[1])

display(dfEmployeeWithSecondArrayElement)

# Print the "Schema" of the Created DataFrame
dfEmployeeWithSecondArrayElement.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # "array ()" Method
# MAGIC * It is possible to "<b>Create</b>" a "<b>New Array Column</b>" by "<b>Merging</b>" the "<b>Data</b>" from "<b>Multiple Columns</b>" in "<b>Each Row</b>" of a "<b>DataFrame</b>" using the "<b>array ()</b>" Method form the "<b>pyspark.sql.functions</b>" Package.

# COMMAND ----------

# DBTITLE 1,"Combine" the "Values" of "Multiple Columns" in "Each Row" to "Create" a "New Derived Column " of "Array Type"
# "Combine" the "Values" of the Columns - "firstName", "lastName", "secondCompanyName", "expInSecondCompany", to "Create" a "Derived Column" of "Array Type" from the DataFrame "dfEmployeeWithSecondArrayElement".
dfEmployeeWithSecondEmploymentInfo = dfEmployeeWithSecondArrayElement.withColumn("secondEmploymentInfo", array(col("firstName"), col("lastName"), col("secondCompanyName"), col("expInSecondCompany")))
display(dfEmployeeWithSecondEmploymentInfo)

# Print the "Schema" of the Created DataFrame
dfEmployeeWithSecondEmploymentInfo.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # "array_contains ()" Method
# MAGIC * It is possible to "<b>Check</b>" if an "<b>Array Column</b>" actually "<b>Contains</b>" a "<b>Value</b>" in "<b>Each Row</b>" of a "<b>DataFrame</b>" using the "<b>array_contains ()</b>" Method form the "<b>pyspark.sql.functions</b>" Package.
# MAGIC * The "<b>array_contains ()</b>" Method "<b>Returns</b>" the following "<b>Values</b>" -
# MAGIC   * "<b>NULL</b>", if the "<b>Array Column</b>" contains "<b>NULL</b>",
# MAGIC   * "<b>True</b>", if the "<b>Array Column</b>" actually "<b>Contains</b>" the "<b>Value</b>" to be "<b>Searched</b>",
# MAGIC   * "<b>False</b>", if the "<b>Array Column</b>" does "<b>Not Contain</b>" the "<b>Value</b>" to be "<b>Searched</b>".

# COMMAND ----------

# DBTITLE 1,Verify If the "Eli Lily" is "Present" in the Column "company" of "Array Type"
dfEmployeeIfWorkingAtLily = dfEmployee.withColumn("IsWorkingAtEliLily", array_contains(col("company"), "Eli Lily"))
display(dfEmployeeIfWorkingAtLily)

# Print the "Schema" of the Created DataFrame
dfEmployeeIfWorkingAtLily.printSchema()
