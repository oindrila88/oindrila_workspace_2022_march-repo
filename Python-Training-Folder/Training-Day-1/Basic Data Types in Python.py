# Databricks notebook source
# MAGIC %md
# MAGIC # Variables

# COMMAND ----------

# Python "Variables" Refer to the "Objects" Created in "Memory", for the Value those are "Assigned To".
a = 10
print(a)

# Assign the Variable "a" to the Variable "b"
b = a
print(a)
print(b)

# Change the Value of the Variable "a" to "20"
a = 20
print(a)
print(b)

# Although, the Value of the Variable "a" has been "Changed" to "20", since, the Variable "b" still Refers to the "Object",
# Created in the "Memory" for the "Integer" Value "10", the Value of the Variable "b" has "Not Changed".

# COMMAND ----------

# MAGIC %md
# MAGIC # Integer and Float

# COMMAND ----------

# Assign the Variable "a" to the Integer Value "5".
a = 5

# Assign the Variable "b" to the Integer Value "2".
b = 2

# COMMAND ----------

# Display Result for "Addition" of "a" and "b"
res = a + b
res

# COMMAND ----------

# Display Result for "Subtraction" of "b" from "a"
res = b - a
res

# COMMAND ----------

# Display Result for "Multiplication" of "a" to "b"
res = a * b
res

# COMMAND ----------

# Display Result for "Division" of "a" by "b"
res = a / b
res

# COMMAND ----------

# Display the "Quotient" for "Division" of "a" by "b"
res = a // b
res

# COMMAND ----------

# Display the "Remainder" for "Division" of "a" by "b"
res = a % b
res

# COMMAND ----------

# Display Result for "Exponential Multiplication" of "b" to "a"
res = b ** a
res

# COMMAND ----------

a = 76
print(a)
print(type(a))

# Convert Variable with "Integer" Value to "Float" Value using the "float ()" Built-In Function
b = float(a)
print(b)
print(type(b))

# COMMAND ----------

a = 67.09456
print(a)
print(type(a))

# Convert Variable with "Float" Value to "Integer" Value using the "int ()" Built-In Function
b = int(a)
print(b)
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC # String

# COMMAND ----------

# Addition of Variable of Type "String" to Variables of Type "Integer" or "Float" will Throw "Error".
myName = 'Oindrila Chakraborty'
print(type(myName))
a = 56

print(myName + a)

# COMMAND ----------

# Multiplying a Variable of Type "String" with a Variable of Type "Integer" will Display the "Concatenation" of the Value of
# the "String" the "Number of Times", provided by the Value of the "Integer", "Without" any "Separator".
myName = "Oindrila Chakraborty Bagchi"
counter = 6
result = myName * counter
result

# COMMAND ----------

# Multiplying "Two" Variables of Type "String" will Throw "Error".
firstName = 'Oindrila'
lastName = "Chakraborty"
result = firstName * lastName
result

# COMMAND ----------

# String Concatenation
firstName = "Oindrila"
lastName = "Chakraborty Bagchi"
address = '''118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

statement = "My Name is : " + firstName + ' ' + lastName + ".\n" + "My Address is : " + address + "."
statement

# COMMAND ----------

# Usage of "f-formatted" String.
firstName = "Oindrila"
lastName = "Chakraborty Bagchi"
address = '''118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

statement = f"My Name is : {firstName} {lastName}.\nMy Address is {address}."
statement

# COMMAND ----------

# Delete a Variable of Type "String" using the "del ()" Built-In Function.
firstName = "Oindrila"
print(firstName)

del(firstName)
# Trying to Display a "Variable" that has been "Deleted" by the "del ()" Built-In Function, will Throw "Error".
print(firstName)

# COMMAND ----------

# Display the "Length" of a Variable of Type "String" using the "len ()" Built-In Function.
address = '''118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

len(address)

# In the above case, the "Length" also included the "New Line" Characters.

# COMMAND ----------

# Convert "All" the "Characters" of a Variable of Type "String" to "Upper Case Letters" using "upper ()" String Method.
address = '''118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

result = address.upper()
result

# COMMAND ----------

# Convert "All" the "Characters" of a Variable of Type "String" to "Lower Case Letters" using "lower ()" String Method.
address = '''118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

result = address.lower()
result

# COMMAND ----------

# Convert "Only" the "First Character" of a Variable of Type "String" to "Upper Case Letter" using "capitalize ()" String
# Method.
address = '''my Address is :
118/H
Narikel Danga North Road
2nd Floor
Kolkata - 700011
West Bengal
India'''

result = address.capitalize()
result
