# Databricks notebook source
# DBTITLE 1,Define a "JSON String"
personHomeJsonString = """
[
    {
        "persons": [
            {
                "name": "Oindrila",
                "age": 34,
                "home": [
                    {
                        "area": "Kolkata",
                        "houseNo": [
                            "118/H",
                            "237Q/1B",
                            "38"
                        ]
                    },
                    {
                        "area": "Chennai",
                        "houseNo": [
                            "208",
                            "34C/8G",
                            "119"
                        ]
                    }
                ]
            },
            {
                "name": "Soumyajyoti",
                "age": 35,
                "home": [
                    {
                        "area": "Kolkata",
                        "houseNo": [
                            "38",
                            "237Q/1B"
                        ]
                    },
                    {
                        "area": "Bangalore",
                        "houseNo": [
                            "111",
                            "88B/4E",
                            "22",
                            "45/1A"
                        ]
                    }
                ]
            }
        ]
    },
    {
        "persons": [
            {
                "name": "Ayan",
                "age": 34,
                "home": [
                    {
                        "area": "Kolkata",
                        "houseNo": [
                            "44",
                            "39C"
                        ]
                    },
                    {
                        "area": "London",
                        "houseNo": [
                            "1C",
                            "221B",
                            "90C"
                        ]
                    }
                ]
            },
            {
                "name": "Dhruba",
                "age": 35,
                "home": [
                    {
                        "area": "Kolkata",
                        "houseNo": [
                            "178",
                            "223/G",
                            "222D",
                            "190A"
                        ]
                    },
                    {
                        "area": "Denver",
                        "houseNo": [
                            "1A",
                            "2C/1B",
                            "90A"
                        ]
                    }
                ]
            }
        ]
    }
]
"""

# COMMAND ----------

# DBTITLE 1,"Load" the "JSON String" to a "DataFrame"
import json

# "Create" a "List" of "Objects" from the "JSON String" using the "json.loads ()" Method 
personHomeJsonList = json.loads(personHomeJsonString)

# "Create" a "DataFrame" from a "List" of "Objects"
dfpersonHome = spark.createDataFrame(personHomeJsonList)

# Display the Content of the Created DataFrame from a "JSON String"
display(dfpersonHome)

# Print the "Schema" of the Created DataFrame from a "JSON String"
dfpersonHome.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # "explode ()" Method
# MAGIC * It is possible to "<b>Create</b>" a "<b>New Row</b>" for "<b>Each Array Element</b>" from a "<b>Given Array Column</b>" using the "<b>explode ()</b>" Method form the "<b>pyspark.sql.functions</b>" Package.

# COMMAND ----------

# DBTITLE 1,"Display" the "Name", "Age" and "Home" of "Each Person" from the "persons" Column
from pyspark.sql.functions import *

# "First" Display the following "Three Different Columns" from the "persons" Column of "Array Data Type" -
# 1. "Name" of "String Data Type",
# 2. "Age" of "Integer Data Type", 
# 3. "Home" of "Array Data Type"
dfPersonInfoWithHome = dfpersonHome.select(
                                            explode(col("persons")).alias("explodedPerson"),
                                            col("explodedPerson.name").alias("Name"),
                                            col("explodedPerson.age").alias("Age"),
                                            col("explodedPerson.home").alias("Home")
                                          )\
                                    .drop("explodedPerson")
display(dfPersonInfoWithHome)

# Print the "Schema" of the Created DataFrame
dfPersonInfoWithHome.printSchema()

# COMMAND ----------

# DBTITLE 1,"Display" the "Name", "Age", "Area" and "House Numbers" of "Each Person" from the "persons" Column
# "Secondly" Display the following "Four Different Columns" from the created DataFrame "dfPersonInfoWithHome" -
# 1. "Name" of "String Data Type",
# 2. "Age" of "Integer Data Type", 
# 3. "Area" of "String Data Type",
# 4. "House Numbers" of "Array Data Type"

# The "Data Type" of the Column "Home" is "String". So, the "String" Value of the "Home" Column needs to be "Converted" into "JSON Value" so that the "explode ()" Method can "Work" on the Column "Home"
dfPersonInfoWithAreaAndHouseNumbers = dfPersonInfoWithHome.select(
                                                                    col("Name"),
                                                                    col("Age"),
                                                                    col("Home").replace("=", ":").alias("explodedHome")

                                                                    
                                                                 )
                                                            
display(dfPersonInfoWithAreaAndHouseNumbers)

# Print the "Schema" of the Created DataFrame
dfPersonInfoWithAreaAndHouseNumbers.printSchema()
