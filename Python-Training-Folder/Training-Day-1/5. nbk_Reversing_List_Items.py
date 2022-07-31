# Databricks notebook source
# MAGIC %md
# MAGIC # "Reversing" the "Items" in "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Using "reverse ()" Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Reverse" the "Elements" of the "List" "In-Place", the "reverse ()" Method is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Reverse" "All" the "Elements" in the "List" of "Integers".
listOfInts.reverse()
print(listOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Reverse" "All" the "Elements" in the "List" of "Strings".
listOfStrings.reverse()
print(listOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Reverse" "All" the "Elements" in the "Homogeneous Nested List" of "Integers".
nestedListOfInts.reverse()
print(nestedListOfInts)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]

# "Reverse" "All" the "Elements" in the "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes.reverse()
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. Using "reversed ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Reverse" the "Elements" of the "List" "In-Place", the "reversed ()" Python Built-In Function is used.
# MAGIC #### This Python Built-In Function Returns the "Reversed List", and, "Does Not Changes" the "List" "In-Place".

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Reverse" "All" the "Elements" in the "List" of "Integers".
reversedListOfInts = list(reversed(listOfInts))
print(reversedListOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Reverse" "All" the "Elements" in the "List" of "Strings".
reversedListOfStrings = list(reversed(listOfStrings))
print(reversedListOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Reverse" "All" the "Elements" in the "Homogeneous Nested List" of "Integers".
reversedNestedListOfInts = list(reversed(nestedListOfInts))
print(reversedNestedListOfInts)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]

# "Reverse" "All" the "Elements" in the "Heterogeneous List" of "Different Object Types".
reversedListOfDifferentTypes = list(reversed(listOfDifferentTypes))
print(reversedListOfDifferentTypes)
