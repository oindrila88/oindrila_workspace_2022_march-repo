# Databricks notebook source
# MAGIC %md
# MAGIC # 1. "Searching" the "Items" in "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Using "in" Operator

# COMMAND ----------

# "Verify" if the Item "It" actually "Exists" in the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
ifExists = "It" in listOfDifferentTypes
print(ifExists)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. Using "not in" Operator

# COMMAND ----------

# "Verify" if the Item "Was" actually "Doesn't Exist" in the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
ifDoesNotExist = "Was" not in listOfDifferentTypes
print(ifDoesNotExist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3. Using "index ()" Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Find" the "Index" of the "First Occurrence" of the Matching Specified "Item" in the “List”, the Method "index ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display the "Index Position" of the Element "88" in the "List" of "Integers".
indexPosition = listOfInts.index(88)
print("First Ocurring Index Position of 88 : ", indexPosition)

# COMMAND ----------

# Not possible to "Find" an "Item" that is "Not Present" in a "List".
# Trying to "Find" the Item "2.8" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "ValueError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
indexPosition = listOfDifferentTypes.remove(2.8)
print("First Ocurring Index Position of 2.8 : ", indexPosition)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4. Using "min ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Find" the "Item" carrying the "Minimum Value" in the “List”, the Python Built-In Function "min ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display the "Smallest Value" in the "List" of "Integers".
minValue = min(listOfInts)
print(minValue)

# COMMAND ----------

# Create First "List" of "Integers".
firstListOfInts = [24, 9, 60, 24, 12, 60, 15, 8, 55]

# Create Second "List" of "Integers".
secondListOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Create Third "List" of "Integers".
thirdListOfInts = [1, 1, 94, 15, 10, 88, 93, 54, 88]

# Display the "List" with the "Minimum First Item" from the "Multiple" "Lists" of "Integers".
listWithMinFirstItem = min(firstListOfInts, secondListOfInts, thirdListOfInts)
print(listWithMinFirstItem)

# COMMAND ----------

# Create an "Empty List".
emptyList = []

# Trying to "Find" the "Smallest Value" in an "Empty List" will Throw "ValueError".
minValue = min(emptyList)
print(minValue)

# COMMAND ----------

# Create an "Empty List".
emptyList = []

# To "Avoid" the "ValueError" When Trying to "Find" the "Smallest Value" in an "Empty List", "Assign" a "Default Value" to the "List", inside the Python Built-In Function "min ()".
minValue = min(emptyList, default = 2)
print(minValue)

# COMMAND ----------

# Not possible to "Find" the "Smallest Value" in a "Heterogeneous List".
# Trying to "Find" the "Smallest Value" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "TypeError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
minValue = min(listOfDifferentTypes)
print("The Smallest Item : ", minValue)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]
minValue = min(nestedListOfInts)
print("The Smallest Item : ", minValue)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5. Using "max ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Find" the "Item" carrying the "Maximum Value" in the “List”, the Python Built-In Function "max ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display the "Largest Value" in the "List" of "Integers".
maxValue = max(listOfInts)
print(maxValue)

# COMMAND ----------

# Create First "List" of "Integers".
firstListOfInts = [24, 9, 60, 24, 12, 60, 15, 8, 55]

# Create Second "List" of "Integers".
secondListOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Create Third "List" of "Integers".
thirdListOfInts = [1, 1, 94, 15, 10, 88, 93, 54, 88]

# Display the "List" with the "Maximum First Item" from the "Multiple" "Lists" of "Integers".
listWithMaxFirstItem = max(firstListOfInts, secondListOfInts, thirdListOfInts)
print(listWithMaxFirstItem)

# COMMAND ----------

# Create an "Empty List".
emptyList = []

# Trying to "Find" the "Largest Value" in an "Empty List" will Throw "ValueError".
maxValue = max(emptyList)
print(maxValue)

# COMMAND ----------

# Create an "Empty List".
emptyList = []

# To "Avoid" the "ValueError" When Trying to "Find" the "Largest Value" in an "Empty List", "Assign" a "Default Value" to the "List", inside the Python Built-In Function "max ()".
maxValue = max(emptyList, default = 2)
print(maxValue)

# COMMAND ----------

# Not possible to "Find" the "Largest Value" in a "Heterogeneous List".
# Trying to "Find" the "Largest Value" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "TypeError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
maxValue = max(listOfDifferentTypes)
print("The Largest Item : ", maxValue)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]
maxValue = max(nestedListOfInts)
print("The Largest Item : ", maxValue)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6. Using "count ()" Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Count" the "Number of Times" an "Item" Appears in the "List", the Method "count ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Display the "Number of Times" the Item "88" Appears in the "List" of "Integers".
countOf88 = listOfInts.count(88)
print(countOf88)

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Trying to "Find" an Item, i.e., "89", that is "Not Present" in the "Homogeneous List" of "Integers", will Return "0".
countOf89 = listOfInts.count(89)
print(countOf89)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]
countOf88 = nestedListOfInts.count(88)
print(countOf88)

# COMMAND ----------

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7],
                                ['Soumyajyoti', 32, 5.9],
                                ['Rahul', 33, 5.10],
                                ["Rohan", 29, 5.7]
                            ]

countOfList = nestedListOfDifferentTypes.count(['Oindrila', 30, 5.7])
print(countOfList)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7. Using "all ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Verify" if "All" the "Items" in the “List” are "Non-Zero", or, "True", the Python Built-In Function "all ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]
ifAllItemsNonZero = all(listOfInts)
print(ifAllItemsNonZero)

listOfInts = [2, 9, 88, 17, 2, 88, 0, 54, 88]
ifAllItemsNonZero = all(listOfInts)
print(ifAllItemsNonZero)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', "False"]
ifAllItemsNonZero = all(listOfDifferentTypes)
print(ifAllItemsNonZero)

listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', False]
ifAllItemsNonZero = all(listOfDifferentTypes)
print(ifAllItemsNonZero)

# COMMAND ----------

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7, False],
                                ['Soumyajyoti', 32, 5.9, True],
                                ['Rahul', 33, 5.10, False],
                                ["Rohan", 29, 5.7, True]
                            ]
ifAllItemsNonZero = all(nestedListOfDifferentTypes)
print(ifAllItemsNonZero)

# COMMAND ----------

# Create an "Empty List".
emptyList = []
ifAllItemsNonZero = all(emptyList)
print(ifAllItemsNonZero)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8. Using "any ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Verify" if "Any" of the "Items" in the “List” is "Zero", or, "False", the Python Built-In Function "any ()" is used.

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [0, 0, 0, 0, 0, 0, 0, 0, 0]
ifAnyItemsNonZero = any(listOfInts)
print(ifAnyItemsNonZero)

listOfInts = [0, 0, 0, 0, 0, 0, 0, 0, -1]
ifAnyItemsNonZero = any(listOfInts)
print(ifAnyItemsNonZero)

listOfInts = [0, 0, 88, 0, 0, 0, 0, 0, 0]
ifAnyItemsNonZero = any(listOfInts)
print(ifAnyItemsNonZero)

# COMMAND ----------

# Create a "Homogeneous List" of "None".
listOfNone = [None, None, None, None, None, None, None, None, None]
ifAnyItemsNonZero = any(listOfNone)
print(ifAnyItemsNonZero)

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = [None, None, None, None, None, False, None, None, None]
ifAnyItemsNonZero = any(listOfDifferentTypes)
print(ifAnyItemsNonZero)

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = [None, None, None, None, None, False, True, None, None]
ifAnyItemsNonZero = any(listOfDifferentTypes)
print(ifAnyItemsNonZero)

# COMMAND ----------

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7, True],
                                ['Soumyajyoti', 32, 5.9, True],
                                ['Rahul', 33, 5.10, True],
                                ["Rohan", 29, 5.7, True]
                            ]
ifAnyItemsNonZero = any(nestedListOfDifferentTypes)
print(ifAnyItemsNonZero)

# COMMAND ----------

# Create an "Empty List".
emptyList = []
ifAnyItemsNonZero = any(emptyList)
print(ifAnyItemsNonZero)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. "Sorting" the "Items" in "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Using "sort ()" Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Sort" the "Values" of "All" the "Elements" in a "List", the "sort ()" Method is used.
# MAGIC #### By default, "Python" performs "Sorting" in "Ascending" Order.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Integers".
listOfInts.sort()
print(listOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings".
listOfStrings.sort()
print(listOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Sort" the "Values" of "All" the "Elements" in the "Homogeneous Nested List" of "Integers".
nestedListOfInts.sort()
print(nestedListOfInts)

# COMMAND ----------

# Not possible to "Sort" "All" the "Items" in a "Heterogeneous List".
# Trying to "Sort" "All" the "Items" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "TypeError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
listOfDifferentTypes.sort()
print(listOfDifferentTypes)

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Integers" in "Descending Order".
listOfInts.sort(reverse = True)
print(listOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in "Descending Order".
listOfStrings.sort(reverse = True)
print(listOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Sort" the "Values" of "All" the "Elements" in the "Homogeneous Nested List" of "Integers" in "Descending Order".
nestedListOfInts.sort(reverse = True)
print(nestedListOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in the "Order" of the "Length" of "Each Item".
listOfStrings.sort(key = len)
print(listOfStrings)

# COMMAND ----------

# Not Providing any Argument to the Python Built-In Function "len ()" will Throw "TypeError".
# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']
lengthOfList = len()

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in the "Descending Order" of the "Length" of "Each Item".
listOfStrings.sort(key = len, reverse = True)
print(listOfStrings)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Using "sorted ()" Python Built-In Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Sort" the "Values" of "All" the "Elements" in a "List", the "sorted ()" Python Built-In Function is used.
# MAGIC #### By default, "Python" performs "Sorting" in "Ascending" Order.
# MAGIC #### This Python Built-In Function Returns the "Sorted List".

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Integers".
sortedListOfInts = sorted(listOfInts)
print(sortedListOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings".
sortedListOfStrings = sorted(listOfStrings)
print(sortedListOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Sort" the "Values" of "All" the "Elements" in the "Homogeneous Nested List" of "Integers".
sortedNestedListOfInts = sorted(nestedListOfInts)
print(sortedNestedListOfInts)

# COMMAND ----------

# Not possible to "Sort" "All" the "Items" in a "Heterogeneous List".
# Trying to "Sort" "All" the "Items" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "TypeError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
sortedListOfDifferentTypes = sorted(listOfDifferentTypes)
print(sortedListOfDifferentTypes)

# COMMAND ----------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Integers" in "Descending Order".
sortedListOfInts = sorted(listOfInts, reverse = True)
print(sortedListOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in "Descending Order".
sortedListOfStrings = sorted(listOfStrings, reverse = True)
print(sortedListOfStrings)

# COMMAND ----------

# Create a "Homogeneous Nested List" of "Integers".
nestedListOfInts = [[17, 2, 87], [2, 9, 88], [14, 8, 93]]

# "Sort" the "Values" of "All" the "Elements" in the "Homogeneous Nested List" of "Integers" in "Descending Order".
sortedNestedListOfInts = sorted(nestedListOfInts, reverse = True)
print(sortedNestedListOfInts)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in the "Order" of the "Length" of "Each Item".
sortedListOfStrings = sorted(listOfStrings, key = len)
print(sortedListOfStrings)

# COMMAND ----------

# Create a "List" of "Strings".
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']

# "Sort" the "Values" of "All" the "Elements" in the "List" of "Strings" in the "Descending Order" of the "Length" of "Each Item".
sortedListOfStrings = sorted(listOfStrings, key = len, reverse = True)
print(sortedListOfStrings)
