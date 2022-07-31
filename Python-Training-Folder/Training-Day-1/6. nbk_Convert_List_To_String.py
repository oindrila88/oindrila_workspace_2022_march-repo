# Databricks notebook source
# MAGIC %md
# MAGIC # "Convert" a "List" to a "String"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Using "Programmatically"

# COMMAND ----------

# MAGIC %md
# MAGIC #### "Iterate" through the "List" and "Keep Adding" the "Element" for "Every Index" in an "Empty String".
# MAGIC #### Only the "List", Having "All" the "Elements" of "String" Data Type, can be "Converted" to a "String" using this Technique.

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Initialize" an "Empty String".
convertedString = "" 
    
# "Traverse" in the "String".  
for string in listOfStrings: 
    convertedString += string  + ','
    
print(convertedString)

# COMMAND ----------

# Not possible to "Convert" a "List", Having "All" the "Items" of "Integer" Data Type to a "String".
# Trying to "Convert" "All" the "Items" of an "Homogeneous List" of "Integers" will Throw "TypeError".

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Initialize" an "Empty String".
convertedString = "" 
    
# "Traverse" in the "String".  
for ints in listOfInts: 
    convertedString += ints  
    
print(convertedString)

# COMMAND ----------

# Not possible to "Convert" a "List", Having "All" the "Items" of a "Heterogeneous List" of "Different" "Data Types".
# Trying to "Convert" "All" the "Items" of an "Homogeneous List" of "Integers" will Throw "TypeError".

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Initialize" an "Empty String".
convertedString = "" 
    
# "Traverse" in the "String".  
for ints in listOfInts: 
    convertedString += ints  
    
print(convertedString)
