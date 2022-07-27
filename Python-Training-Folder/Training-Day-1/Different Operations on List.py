# Databricks notebook source
# MAGIC %md
# MAGIC # 1. "Iterating" through the "Items" in "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Using "for-in" Loop

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display "Each" of the "Items" from the "List" of "Integers".
for intVal in listOfInts:
    print(intVal)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]

# Display "Each" of the "Items" from the "List" of "Different Object Types".
for differentVal in listOfDifferentTypes:
    print(differentVal)

# COMMAND ----------

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7],
                                ['Soumyajyoti', 32, 5.9],
                                ['Rahul', 33, 5.10],
                                ["Rohan", 29, 5.7]
                            ]

# Display "Each" of the "Items" from the "Nested List" of "Different Object Types".
for differentVal in nestedListOfDifferentTypes:
    print(differentVal)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. Using "enumerate ()" Python Built-In Function

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display "Each" of the "Items" from the "List" of "Integers".
enumListOfInts = enumerate(listOfInts)
for enumIntVal in enumListOfInts:
    print(enumIntVal)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]

# Display "Each" of the "Items" from the "List" of "Different Object Types".
enumListOfDifferentTypes = enumerate(listOfDifferentTypes)
for enumDifferentVal in enumListOfDifferentTypes:
    print(enumDifferentVal)

# COMMAND ----------

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7],
                                ['Soumyajyoti', 32, 5.9],
                                ['Rahul', 33, 5.10],
                                ["Rohan", 29, 5.7]
                            ]

# Display "Each" of the "Items" from the "Nested List" of "Different Object Types".
enumNestedListOfDifferentTypes = enumerate(nestedListOfDifferentTypes)
for enumDifferentVal in enumNestedListOfDifferentTypes:
    print(enumDifferentVal)
