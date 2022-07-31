# Databricks notebook source
# MAGIC %md
# MAGIC # What is "List"?

# COMMAND ----------

# MAGIC %md
# MAGIC ### "List" is a "Data Type" that "Stores" Multiple "Objects" of "Same" or "Different" "Data Types", separated by "Commas", and, "Enclosed" by a "Square Bracket", i.e., "[]".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Nested List"?

# COMMAND ----------

# MAGIC %md
# MAGIC ### It is possible to "Create" a "List", where "All" of the "Elements" of a "List" can be "List" itself, i.e., it is possible to "Create" a "List of Lists".

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Create "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Using "Subscript Operator"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Integers"

# COMMAND ----------

listOfInts = [2, 9, 88, 17, 2, 87]
print(listOfInts)
print(type(listOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Floats"

# COMMAND ----------

listOfFloats = [6.9832, 12.54, 3.87, 98.54309, 2.32, 65.430912543]
print(listOfFloats)
print(type(listOfFloats))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Strings"

# COMMAND ----------

listOfStrings = ['My', "Name", 'is', "Oindrila", 'Chakraborty', "Bagchi"]
print(listOfStrings)
print(type(listOfStrings))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Heterogeneous List" of "Different Object Types"

# COMMAND ----------

listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
print(listOfDifferentTypes)
print(type(listOfDifferentTypes))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Integers"

# COMMAND ----------

nestedListOfInts = [[2, 9, 88], [17, 2, 87], [14, 8, 93]]
print(nestedListOfInts)
print(type(nestedListOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Floats"

# COMMAND ----------

nestedListOfFloats = [[6.9832, 12.54], [3.87, 98.54309, 2.32], [65.430912543, 1.564, 4.098, 12.8904]]
print(nestedListOfFloats)
print(type(nestedListOfFloats))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Strings"

# COMMAND ----------

nestedListOfStrings = [
                        ['Oindrila', 'Chakraborty', "Bagchi"],
                        ['Soumyajyoti', 'Bagchi'],
                        ['Rahul', 'Roy', 'Chowdhury'],
                        ['Debjani', "Pal", 'Nayek', "Chowdhury"]
                    ]
print(nestedListOfStrings)
print(type(nestedListOfStrings))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Heterogeneous Nested List" of "Different Object Types"

# COMMAND ----------

nestedListOfDifferentTypes = [
                                ['Oindrila', 30, 5.7],
                                ['Soumyajyoti', 32, 5.9],
                                ['Rahul', 33, 5.10],
                                ["Rohan", 29, 5.7]
                            ]
print(nestedListOfDifferentTypes)
print(type(nestedListOfDifferentTypes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. Using "List Constructor"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Integers"

# COMMAND ----------

listOfInts = list([2, 9, 88, 17, 2, 87])
print(listOfInts)
print(type(listOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Floats"

# COMMAND ----------

listOfFloats = list([6.9832, 12.54, 3.87, 98.54309, 2.32, 65.430912543])
print(listOfFloats)
print(type(listOfFloats))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Strings"

# COMMAND ----------

listOfStrings = list(['My', "Name", 'is', "Oindrila", 'Chakraborty', "Bagchi"])
print(listOfStrings)
print(type(listOfStrings))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Heterogeneous List" of "Different Object Types"

# COMMAND ----------

listOfDifferentTypes = list(['Hi!', 22, 'Is', "It", 3.56, True])
print(listOfDifferentTypes)
print(type(listOfDifferentTypes))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Integers"

# COMMAND ----------

nestedListOfInts = list([[2, 9, 88], [17, 2, 87], [14, 8, 93]])
print(nestedListOfInts)
print(type(nestedListOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Floats"

# COMMAND ----------

nestedListOfFloats = list([
                            [6.9832, 12.54],
                            [3.87, 98.54309, 2.32],
                            [65.430912543, 1.564, 4.098, 12.8904]                       
                          ])
print(nestedListOfFloats)
print(type(nestedListOfFloats))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous Nested List" of "Strings"

# COMMAND ----------

nestedListOfStrings = list([
                                ['Oindrila', 'Chakraborty', "Bagchi"],
                                ['Soumyajyoti', 'Bagchi'],
                                ['Rahul', 'Roy', 'Chowdhury'],
                                ['Debjani', "Pal", 'Nayek', "Chowdhury"]
                            ])
print(nestedListOfStrings)
print(type(nestedListOfStrings))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Heterogeneous Nested List" of "Different Object Types"

# COMMAND ----------

nestedListOfDifferentTypes = list([
                                    ['Oindrila', 30, 5.7],
                                    ['Soumyajyoti', 32, 5.9],
                                    ['Rahul', 33, 5.10],
                                    ["Rohan", 29, 5.7]
                                ])
print(nestedListOfDifferentTypes)
print(type(nestedListOfDifferentTypes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3. Using "List Comprehension"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Integers"

# COMMAND ----------

# Create a "List" of "Integers" of Numbers between "70" and "79". "79" will be "Excluded" in the "Output".
listOfInts = [number for number in range(70, 79)]
print(listOfInts)
print(type(listOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create "Homogeneous List" of "Strings"

# COMMAND ----------

# Create a "List" of "Strings", where "Each Element" would be the "First Letter" of "Every Element" of the List "listOfCountries".
listOfCountries = ['India', "Japan", 'South Korea', "Thailand", 'Vietnam', "Indonesia"]
print(listOfCountries)
print(type(listOfCountries))

listOfFirstLetters = [country[0] for country in listOfCountries]
print(listOfFirstLetters)
print(type(listOfFirstLetters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a "Homogeneous List" of "Integers" from a "Heterogeneous List" of "Different Object Types"

# COMMAND ----------

# Create a "List" of "Only Integers" from the Heterogeneous List "listOfDifferentTypes".
listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', False]
print(listOfDifferentTypes)
print(type(listOfDifferentTypes))

listOfInts = [listElement for listElement in listOfDifferentTypes if str(listElement).isnumeric()]
print(listOfInts)
print(type(listOfInts))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Access "Items" of "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Using "List Indexing"

# COMMAND ----------

# MAGIC %md
# MAGIC #### "List" in "Python" follows "Zero-Based Indexing". i.e., the "First Element" of a "List" has the "Index Position" as "0", the "Second Element" of the "List" has the "Index Position" as "1", and, so on.

# COMMAND ----------

# Access the "Fifth Item" present at the Index Position "4" of the List "listOfInts".
listOfInts = [2, 9, 88, 17, 2, 87]
print(listOfInts[4])

# COMMAND ----------

# Not possible to access "Index" that is beyond the "List Index Range".
# Trying to access the "Ninth Item" present at the Index Position "8" of the List "listOfInts" will Throw "IndexError".
listOfInts = [2, 9, 88, 17, 2, 87]
print(listOfInts[8])

# COMMAND ----------

# Not possible to access "Index" that is "Not Integer".
# Trying to access the Index Position "2.8" of the List "listOfInts" will Throw "TypeError".
listOfInts = [2, 9, 88, 17, 2, 87]
print(listOfInts[2.8])

# COMMAND ----------

# Access the Nested Item "Soumyajyoti" present at the Index Position "1, 0" of the List "nestedListOfDifferentTypes".

nestedListOfDifferentTypes = list([
                                    ['Oindrila', 30, 5.7],
                                    ['Soumyajyoti', 32, 5.9],
                                    ['Rahul', 33, 5.10],
                                    ["Rohan", 29, 5.7]
                                ])
print(nestedListOfDifferentTypes[1][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Using "Negative Indexing"

# COMMAND ----------

# MAGIC %md
# MAGIC #### In the "Negative Indexing" of a "List", the "Last Element" of a "List" has the "Index Position" as "-1", the "Second Last Element" of the "List" has the "Index Position" as "-2", and, so on.

# COMMAND ----------

# Access the "Second Last Item" present at the Index Position "-2" of the List "listOfInts".
listOfInts = [2, 9, 88, 17, 2, 87]
print(listOfInts[-2])

# COMMAND ----------

# Access the Nested Item "32" present at the Negative Index Position "-3, -2" of the List "nestedListOfDifferentTypes".

nestedListOfDifferentTypes = list([
                                    ['Oindrila', 30, 5.7],
                                    ['Soumyajyoti', 32, 5.9],
                                    ['Rahul', 33, 5.10],
                                    ["Rohan", 29, 5.7]
                                ])
print(nestedListOfDifferentTypes[-3][-2])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3. Using "List Slicing"

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to "Display" a "Subset" of the "Elements" of a "List" using the "List Sciling" Technique. The "Element" of the "List", which would be at the "Index Position", mentioned as the "End" of the "Slicing" would be "Excluded" in the "Output".

# COMMAND ----------

listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', False]

# Access the "Items" from the "First Item" till the "Fifth Item" present at the Index Position "4" of the List "listOfDifferentTypes".
print(listOfDifferentTypes[:4])

# Access the "Items" from the "Third Item" present at the Index Position "2" till the "Last Item" of the List "listOfDifferentTypes".
print(listOfDifferentTypes[2:])

# COMMAND ----------

listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# -----------------------------------------------------------------------------------------------------------------------------

# "Display" from the "Second Item" at Index Position "1", till the "Fifth Item" at Index Position "4". The "Fifth Item" at Index Position "4" would be "Excluded" in the "Output".
print(listOfInts[1:4])

# -----------------------------------------------------------------------------------------------------------------------------

# If the "Index Position", mentioned as the "End" of the "Slicing" Does "Not Exist" in the "List", then the "Items", from the "Index Position", mentioned as the "Start" of the "Slicing", till the "Last Item" of the "List" are "Displayed".
print(listOfInts[1:10])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" the "Items" from an "Index Position", mentioned as the "Start" of the "Slicing" till the "Last Item" of a "List", i.e., Including the "Last Item", no "Index Position" Should be Provided as the "End" of the "Slicing". The "End" Should be Kept as "Blank".

# "Display" from the "Fourth Item" at Index Position "3", till the "Last Item" of the "List".
print(listOfInts[3:])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" from the "First Item" of a "List" till the "Index Position", mentioned as the "End" of the "Slicing" of a "List", i.e., Excluding the "Item" Present at the "Index Position" mentioned as the "End" of the "Slicing", no "Index Position" Should be Provided as the "Start" of the "Slicing". The "Start" Should be Kept as "Blank".

# "Display" from the "First Item" of the "List", till the "Fifth Item" at Index Position "4". The "Fifth Item" at Index Position "4" would be "Excluded" in the "Output".
print(listOfInts[:4])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" "All" the "Items" of a "List" using "List Slicing", no "Index Position" Should be Provided "Both" as the "Start" and "End" of the "Slicing". Both the "Start" and "End" Should be Kept as "Blank".
print(listOfInts[:])

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to "Display" a "Subset" of the "Elements" of a "List" using the "List Sciling" Technique, where the "Elements" would be Present at a "Specific Interval". The "Element" of the "List", which would be at the "Index Position", mentioned as the "End" of the "Slicing" would be "Excluded" in the "Output".
# MAGIC #### The "Interval" is mentioned as the "Step" of the "Slicing".

# COMMAND ----------

listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', False]

# Access Every "Second Item" from the "Second Item" present at the Index Position "1" till the "Eighth Item" present at the Index Position "7" of the List "listOfDifferentTypes".
print(listOfDifferentTypes[1:7:2])

# Access Every "Second Item" from the "Second Last Item" present at the Index Position "-2" till the "Eighth Last Item" present at the Index Position "-8" of the List "listOfDifferentTypes".
print(listOfDifferentTypes[-2:-8:-2])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. "Lists" are "Mutable"

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to "Change" the Value of the "Item" of a "List" at any "Index Position".

# COMMAND ----------

listOfDifferentTypes = [2, 5.6, 'Is', "It", 17, True, 2, 'S', False]

# "Change" the Value of the "Sixth Item" present at the Index Position "5" to "False" of the List "listOfDifferentTypes".
listOfDifferentTypes[5] = False
print(listOfDifferentTypes)

# "Change" the Value of the "Sixth Last Item" present at the Index Position "-6" to "Was" of the List "listOfDifferentTypes".
listOfDifferentTypes[-6] = "Was"
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to "Assign" "Multiple New Elements" as another "List" to a "Subset" of the "Elements" of a "List" using the "List Slicing" Technique.

# COMMAND ----------

# -----------------------------------------------------------------------------------------------------------------------------

listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Here, the "Number of Elements" to "Assign" in the "List", i.e., "4" is "Greater Than" the "Summation" of the "Index Positions", mentioned in the "Slicing", i.e., "2". Hence, the "List" got "Expanded".

listOfInts[1:3] = [93, 14, 8, 9]
print(listOfInts)

# -----------------------------------------------------------------------------------------------------------------------------

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# Here, the "Number of Elements" to "Assign" in the "List", i.e., "2" is "Less Than" the "Summation" of the "Index Positions", mentioned in the "Slicing", i.e., "3". Hence, the "List" got "Shrunk".

listOfInts[1:4] = [93, 14]
print(listOfInts)

# -----------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trying to "Assign" a Single "New Item" to a "Subset" of the "Items" of a "List" using the "List Sciling" Technique will Throw "Error".

# COMMAND ----------

listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

listOfInts[1:4] = 93
print(listOfInts)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trying to "Assign" the "Items" beyond the "Indices Range" of a “List” would Throw "Error”

# COMMAND ----------

listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

listOfInts[14] = 93
print(listOfInts)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. "Add" the "Items" in the "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1. Using "append ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Add" an "Object" to the "End" of the "List", the Method "append ()" is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# "Append" a Boolean Value "False" to the "End" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfDifferentTypes.append(False)
print(listOfDifferentTypes)

# COMMAND ----------

# "Append" another "List" Value "['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']" to the "End" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']
listOfDifferentTypes.append(listOfStrings)
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trying to "Append" "More Than One Object" to the "End" of the "List" of Multiple "Objects" of "Different" "Data Types" wil  Throw "Error", because, the "append ()" Method "Takes Only One Argument" of Any "Data Type".

# COMMAND ----------

listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfDifferentTypes.append(False, "Ryugamine")
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2. Using "extend ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Add" all the "Items" of an "Iterable" to the "End" of the "List", the Method "extend ()" is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# "Append" "All" the "Items" of another "List" to the "End" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']
listOfDifferentTypes.extend(listOfStrings)
print(listOfDifferentTypes)

# COMMAND ----------

# "Append" "All" the "Items" of another "Nested List" to the "End" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
nestedListOfDifferentTypes = list([
                                    ['Oindrila', 30, 5.7],
                                    ['Soumyajyoti', 32, 5.9],
                                    ['Rahul', 33, 5.10],
                                    ["Rohan", 29, 5.7]
                                ])
listOfDifferentTypes.extend(nestedListOfDifferentTypes)
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3. Using "List Concatenation"

# COMMAND ----------

# Create a "Homogeneous List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 87]

# Create a "Homogeneous List" of "Floats".
listOfFloats = [6.9832, 12.54, 3.87, 98.54309, 2.32, 65.430912543]

# "Concatenate" the Two "Lists".
result = listOfInts + listOfFloats
print(result)

# COMMAND ----------

# Create a "Heterogeneous List" of "Different Object Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]

# Create a "Heterogeneous Nested List" of "Different Object Types".
nestedListOfDifferentTypes = list([
                                    ['Oindrila', 30, 5.7],
                                    ['Soumyajyoti', 32, 5.9],
                                    ['Rahul', 33, 5.10],
                                    ["Rohan", 29, 5.7]
                                ])

# "Concatenate" the Two "Lists".
result = listOfDifferentTypes + nestedListOfDifferentTypes
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4. Using "Multiplication Operator"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Multiplying a "List" with a Variable of Type "Integer" will Display the "Concatenation" of the "All" the "Elements" of the "List" the "Number of Times", provided by the Value of the "Integer".

# COMMAND ----------

# Create a "Homogeneous List" of "Strings".
listOfStrings = ["Hi", "Hello", "Bye"]

# Assign the Integer Value "5" to the Variable "a".
a = 5

# "Multiply" the List "listOfStrings" with the Integer Variable "a".
result = listOfStrings * a
print(result)

# COMMAND ----------

# Create a "Homogeneous List" of "Strings".
listOfStrings = ["Hi", "Hello", "Bye"]

# Assign the Integer Value "5" to the Variable "a".
a = 5

# "Multiply" the List "listOfStrings" with the Integer Variable "a" to Create a "Homogeneous Nested List" of "Strings".
result = [listOfStrings] * a
print(result)

# COMMAND ----------

# Create a "Homogeneous List" of "Strings".
listOfStrings = ["Hi", "Hello", "Bye"]

# Assign the Integer Value "5" to the Variable "a".
a = 5

# "Multiply" the List "listOfStrings" with the Integer Variable "a" to Create a "Homogeneous Nested List" of "Strings".
result = [listOfStrings] * a
print(result)

# Changing Value of One "Nested Item" will Change the Values of All the "Nested Items".
result[0][2] = 'What'
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5. Using "insert ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Add" an "Object" at the specified "Index" of the "List", the Method "insert ()" is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# "Insert" a Boolean Value "False" at the "Third Position", i.e., at the Index Position "2" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfDifferentTypes.insert(2, False)
print(listOfDifferentTypes)

# COMMAND ----------

# "Insert" another "List" Value "['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']" at the "Third Position", i.e., at the Index Position "2" of the "List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfStrings = ['My', 'Name', 'is', 'Oindrila', 'Chakraborty', 'Bagchi']
listOfDifferentTypes.insert(2, listOfStrings)
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6. Using "List Slicing Syntax"

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to Insert Multiple Items at a specified "Index" in a “List”, by using the Slice Assignment.

# COMMAND ----------

# "Create" a "Sub-List" from the "Second Item" at Index Position "1", till the "Fifth Item" at Index Position "4". The "Fifth Item" at Index Position "4" would be "Excluded" in the "Output".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
result = listOfDifferentTypes[1:4]
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. "Delete" the "Items" from the "List"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1. Using "del" Operator

# COMMAND ----------

# "Delete" the "Second Item" present at the Index Position "1".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
del listOfDifferentTypes[1]
print(listOfDifferentTypes)

# COMMAND ----------

# "Delete" the "Items" from the "Second Item" at Index Position "1", till the "Fifth Item" at Index Position "4". The "Fifth Item" at Index Position "4" would "Not Be Deleted" in the "Output".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
del listOfDifferentTypes[1:4]
print(listOfDifferentTypes)

# COMMAND ----------

# "Delete" the "Entire List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
del listOfDifferentTypes
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2. Using "remove ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Delete" the "First Matching Item" from the "List", the Method "remove ()" is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# "Delete" the Item "22" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
listOfDifferentTypes.remove(22)
print(listOfDifferentTypes)

# COMMAND ----------

# Not possible to "Delete" an "Item" that is "Not Present" in a "List".
# Trying to "Delete" the Item "2.8" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "ValueError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
listOfDifferentTypes.remove(2.8)
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3. Using "pop ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Delete" an "Item" from the Specified "Index" from the "List", the Method "pop ()" is used.
# MAGIC #### This Method Returns the "Deleted Item", and, it also "Changes" the "List" "In-Place".

# COMMAND ----------

# "Delete" the "Second Item" present at the Index Position "1".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
deletedItem = listOfDifferentTypes.pop(1)
print("Deleted Item : ", deletedItem)
print("Updated List : ", listOfDifferentTypes)

# COMMAND ----------

# If no Argument is passed, the "pop ()" List Method "Deletes" and "Returns" the "Last Item" in the "List".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
deletedItem = listOfDifferentTypes.pop()
print("Deleted Item : ", deletedItem)
print("Updated List : ", listOfDifferentTypes)

# COMMAND ----------

# Not possible to Delete an "Index" that is beyond the "List Index Range".
# Trying to "Delete" the Item, present at the Index Position "18" from the "Heterogeneous List" of Multiple "Objects" of "Different" "Data Types" will Throw "IndexError".
listOfDifferentTypes = ['Hi!', 22, 'Is', 22, "It", 3.56, True, 22]
deletedItem = listOfDifferentTypes.pop(18)
print("Deleted Item : ", deletedItem)
print("Updated List : ", listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4. Using "clear ()" List Method

# COMMAND ----------

# MAGIC %md
# MAGIC #### To "Delete" "All Items" from a “List”, the Method "clear ()" is used.
# MAGIC #### This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# COMMAND ----------

# "Delete" the "Entire List" of Multiple "Objects" of "Different" "Data Types".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfDifferentTypes.clear()
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5. Using "List Slicing Syntax"

# COMMAND ----------

# MAGIC %md
# MAGIC #### It is possible to "Delete" the "Items" from a "List" by "Assigning" an “Empty List” to a "Slice" of its "Items".

# COMMAND ----------

# "Delete" the "Items" from the "Second Item" at Index Position "1", till the "Fifth Item" at Index Position "4". The "Fifth Item" at Index Position "4" would "Not Be Deleted" in the "Output".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
listOfDifferentTypes[1:4] = []
print(listOfDifferentTypes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform the "Summation" of "All" the "Elements" of a "List"

# COMMAND ----------

# Display the "Summation" of "All" the "Elements" of a "List" using "sum ()" Built-In Function. The "sum ()" Built-In Function
# "Works" for "Numeric" Elements "Only".

# Display the "Summation" of "All" the "Elements" of a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 87]
result = sum(listOfInts)
print(result)

# Display the "Summation" of "All" the "Elements" of a "List" of "Floats".
listOfFloats = [6.9832, 12.54, 3.87, 98.54309, 2.32, 65.430912543]
result = sum(listOfFloats)
print(result)

# Trying to Display the "Summation" of "All" the "Elements" of a "List" of "Strings" will Throw "Error".
listOfStrings = ['My', "Name", 'is', "Oindrila", 'Chakraborty', "Bagchi"]
result = sum(listOfStrings)
print(result)

# Trying to Display the "Summation" of "All" the "Elements" of a "List" of Multiple "Objects" of "Different" "Data Types"
# will Throw "Error".
listOfDifferentTypes = ['Hi!', 22, 'Is', "It", 3.56, True]
result = sum(listOfDifferentTypes)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## "Reverse" the "Positions" of "All" the "Elements" in a "List"

# COMMAND ----------

# To "Reverse" the "Position" of "All" the "Elements" in a "List", the "reverse ()" Method is used.
# This Method "Does Not Return Anything", it "Changes" the "List" "In-Place".

# Create a "List" of "Integers".
listOfInts = [2, 9, 88, 17, 2, 88, 93, 54, 88]

# "Reverse" the "Position" of "All" the "Elements" in the "List" of "Integers".
listOfInts.reverse()
print(listOfInts)
