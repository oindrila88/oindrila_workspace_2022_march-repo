# Databricks notebook source
# MAGIC %md
# MAGIC # Assign a "String" with Values

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is Oindrila Chakraborty Bagchi" using "Single Quotes".
a = 'My Name is Oindrila Chakraborty Bagchi'
print(type(a))

# Assign the Variable "b" to the he "String" Value "My Name is Oindrila Chakraborty Bagchi" using "Double Quotes".
b = "My Name is Oindrila Chakraborty Bagchi"
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC # Assign an "Empty String"

# COMMAND ----------

# Assign the Variable "a" to an "Empty String"
a = ''
print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC # Suppressing the "Meaning" of "Special Character" using "Backslash", i.e., "\\"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suppressing the "Meaning" of "Quotes" using "Backslash", i.e., "\\"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Single Quotes".
a = 'My Name is \'Oindrila Chakraborty Bagchi\'.'
print(a)

# Assign the Variable "b" to the he "String" Value "My Name is "Oindrila Chakraborty Bagchi"." using "Double Quotes".
b = "My Name is \"Oindrila Chakraborty Bagchi\"."
print(b)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suppressing the "Meaning" of "New Line Character" using "Backslash", i.e., "\\"

# COMMAND ----------

address = '118/H \
Narikel Danga North Road \
2nd Floor \
Kolkata - 700011 \
West Bengal \
India'

print(address)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suppressing the "Meaning" of "Backslash" using "Backslash", i.e., "\\"

# COMMAND ----------

address = '118\\H \
Narikel Danga North Road \
2nd Floor 
\
Kolkata - 700011 \
West Bengal \
India'

print(address)

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw Strings

# COMMAND ----------

address = r'118\H \
Narikel Danga North Road \
2nd Floor \
Kolkata - 700011 \
West Bengal \
India'

print(address)

name = R'My name is Oindrila\tChakraborty\tBagchi'
print(name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Include Quote Characters as Part of the "String" Itself

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the Other Quotes to Assign the "String"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Double Quotes".
a = "My Name is 'Oindrila Chakraborty Bagchi'."
print(a)

# Assign the Variable "b" to the he "String" Value "My Name is "Oindrila Chakraborty Bagchi"." using "Single Quotes".
b = 'My Name is "Oindrila Chakraborty Bagchi".'
print(b)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using "Escape Sequence"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Single Quotes" with Escape
# Sequence, i.e., Backslash "\".
a = 'My Name is \'Oindrila Chakraborty Bagchi\'.'
print(a)

# Assign the Variable "b" to the he "String" Value "My Name is "Oindrila Chakraborty Bagchi"." using "Double Quotes" with Escape
# Sequence, i.e., Backslash "\".
b = "My Name is \"Oindrila Chakraborty Bagchi\"."
print(b)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using "Triple-Quoted String"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''
print(a)

# Assign the Variable "b" to the he "String" Value "My Name is "Oindrila Chakraborty Bagchi"." using "Triple Double Quotes".
b = """My Name is "Oindrila Chakraborty Bagchi"."""
print(b)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using "Triple-Quoted String" to "Assign" the "Multi-Line String"

# COMMAND ----------

address = '''118\H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

print(address)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Indexing" of "String"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Positive Indexing

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# "Display" the "First Character" of the "String", i.e., the "Character" present at the Index Position "0".
print(a[0])

# "Display" the "Fourteenth Character" of the "String", i.e., the "Character" present at the Index Position "13".
print(a[13])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Negative Indexing

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# "Display" the "Last Character" of the "String", i.e., the "Character" present at the Index Position "-1".
print(a[-1])

# "Display" the "Fourteenth Last Character" of the "String", i.e., the "Character" present at the Index Position "-14".
print(a[-14])

# COMMAND ----------

# MAGIC %md
# MAGIC # "Slicing" of "String"

# COMMAND ----------

# It is possible to "Display" a "Sub-String" from a "String" using the "String Sciling" Technique. The "Character" of the
# "String", which would be at the "Index Position", mentioned as the "End" of the "Slicing" would be "Excluded" in the "Output".

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# -----------------------------------------------------------------------------------------------------------------------------

# "Display" from the "Second Character" at Index Position "1", till the "Fifteenth Character" at Index Position "14".
# The "Fifth Element" at Index Position "4" would be "Excluded" in the "Output".
print(a[1:14])

# -----------------------------------------------------------------------------------------------------------------------------

# If the "Index Position", mentioned as the "End" of the "Slicing" Does "Not Exist" in the "String", then the "Characters", from
# the "Index Position", mentioned as the "Start" of the "Slicing", till the "Last Character" of the "String" are "Displayed".
print(a[1:100])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" the "Characters" from an "Index Position", mentioned as the "Start" of the "Slicing" till the "Last Character" of
# a "String", i.e., Including the "Last Character", no "Index Position" Should be Provided as the "End" of the "Slicing". The
# "End" Should be Kept as "Blank".

# "Display" from the "Fourth Character" at Index Position "3", till the "Last Character" of the "String".
print(a[3:])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" from the "First Character" of a "String" till the "Index Position", mentioned as the "End" of the "Slicing" of a
# "String", i.e., Excluding the "Character" Present at the "Index Position" mentioned as the "End" of the "Slicing", no "Index
# Position" Should be Provided as the "Start" of the "Slicing". The "Start" Should be Kept as "Blank".

# "Display" from the "First Character" of the "String", till the "Twentieth Character" at Index Position "19".
# The "Twentieth Character" at Index Position "19" would be "Excluded" in the "Output".
print(a[:19])

# -----------------------------------------------------------------------------------------------------------------------------

# To "Display" "All" the "Characters" of a "String" using "String Slicing", no "Index Position" Should be Provided for "Both"
# the "Start" and "End" of the "Slicing". Both the "Start" and "End" Should be Kept as "Blank".
print(a[:])

# -----------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Slicing" of a "String" using "Step" Increment

# COMMAND ----------

# It is possible to "Display" a "Sub-String" from a "String" using the "String Sciling" Technique, where the "Characters"
# would be Present at a "Specific Interval". The "Character" of the "String", which would be at the "Index Position", mentioned
# as the "End" of the "Slicing" would be "Excluded" in the "Output".
# The "Interval" is mentioned as the "Step" of the "Slicing".

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# -----------------------------------------------------------------------------------------------------------------------------

# "Display" Every "Second Character" "Beginning" from the "First Character" of the "String", i.e., at Index Position "0", till
# the "Twentieth Character" at Index Position "19".
# The "Twentieth Character" at Index Position "19" would be "Excluded" in the "Output".
print(a[:19:2])

# -----------------------------------------------------------------------------------------------------------------------------

# "Display" Every "Second Character" "Beginning" from the "Third Character" of the "String", i.e., at Index Position "2" till
# the "Eighteenth Element" at Index Position "17".
# The "Eightenth Character" at Index Position "17" would be "Excluded" in the "Output".
print(a[2:17:2])

# -----------------------------------------------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC # "Strings" are "Immutable"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# Trying to "Assign" the Character "M" at the Index Position "17" of the "String" will Throw "Error".
a[17] = 'M'

# COMMAND ----------

# MAGIC %md
# MAGIC # "Delete" a "Character" of a "String"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''

# Trying to "Delete" the Character present at the Index Position "17" of the "String" will Throw "Error".
del a[17]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Delete" an Entire "String"

# COMMAND ----------

# Assign the Variable "a" to the "String" Value "My Name is 'Oindrila Chakraborty Bagchi'." using "Triple Single Quotes".
a = '''My Name is 'Oindrila Chakraborty Bagchi'.'''
print(a)

# "Delete" the Entire "String".
del a
print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Format" a "String"

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "% Operator"

# COMMAND ----------

firstName = 'Oindrila'
lastName = "Chakraborty"
age = 30
height = 5.7

print('Hi. My name is %s %s. My age is %d. My height is %f Feet.' %(firstName, lastName, age, height))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "% Operator" for "Floating-Point Numbers"

# COMMAND ----------

firstName = 'Oindrila'
lastName = "Chakraborty"
age = 30
height = 5.7

print('Hi. My name is %s %s. My age is %d. My height is %3.1f Feet.' %(firstName, lastName, age, height))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "format ()" String Method

# COMMAND ----------

firstName = 'Oindrila'
lastName = "Chakraborty"
age = 30
height = 5.7

print('Hi. My name is {} {}. My age is {}. My height is {} Feet.'.format(firstName, lastName, age, height))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "format ()" String Method using "Positional Arguments" Technique

# COMMAND ----------

firstName = 'Oindrila'
lastName = "Chakraborty"
age = 30
height = 5.7

print('Hi. My name is {2} {1}. My age is {3}. My height is {0} Feet.'.format(height, lastName, firstName, age))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "format ()" String Method using "Keyword Arguments" Technique

# COMMAND ----------

print('Hi. My name is {firstName} {lastName}. My age is {age}. My height is {height} Feet.'.format(height = 5.7,
                                                                        lastName = "Chakraborty", 
                                                                        firstName = 'Oindrila',
                                                                        age = 30))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with "format ()" String Method using "Dictionary Arguments" Technique

# COMMAND ----------

dictOin = {
    "age": 30,
    "lastName": "Chakraborty",
    "firstName": 'Oindrila',
    "height": 5.7
}

print('Hi. My name is {firstName} {lastName}. My age is {age}. My height is {height} Feet.'.format(**dictOin))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Format" a "String" with “f-Strings”, i.e., "Literal String Interpolation"

# COMMAND ----------

firstName = 'Oindrila'
lastName = "Chakraborty"
age = 30
height = 5.7

print(f'Hi. My name is {firstName} {lastName}. My age is {age}. My height is {height} Feet.')
