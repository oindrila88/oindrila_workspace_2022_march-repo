# Databricks notebook source
# MAGIC %md
# MAGIC # Basic

# COMMAND ----------

# By default, "Jupyter Notebook" Displays Only the "Last Statement" written in the Notebook. Hence, it is not possible to Display Multiple Values in One Cell.
6
10

# COMMAND ----------

# To Display Multiple Values in One Cell, the "print ()" Built-In Function is used.
print(6)
print(10)

# COMMAND ----------

# It is also possible to Display Multiple Values in One Cell, using a Single "print ()" Built-In Function.
print(6, 10, 16, 40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Integer

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1. Decimal

# COMMAND ----------

# Assign the Variable "a" to the "Decimal" Integer Value "5".
a = 5
print(type(a))

# Assign the Variable "b" to the "Decimal" Integer Value "2".
b = 2
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2. Binary

# COMMAND ----------

# Assign the Variable "a" to the "Binary" Value "1001".
a = 0b1001
print(type(a))

# Assign the Variable "b" to the "Binary" Value "101011".
b = 0B101011
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3. Octal

# COMMAND ----------

# Assign the Variable "a" to the "Octal" Value "0o1060".
a = 0o1060
print(type(a))

# Assign the Variable "b" to the "Octal" Value "0O1254".
b = 0O1254
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4. Hexadecimal

# COMMAND ----------

# Assign the Variable "a" to the "Hexadecimal" Value "0x109F".
a = 0x109F
print(type(a))

# Assign the Variable "b" to the "Hexadecimal" Value "0X234DE".
b = 0X234DE
print(type(b))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Float

# COMMAND ----------

# Assign the Variable "a" to the Float Value "67.098".
a = 67.098
print(type(a))

# Assign the Variable "b" to the Float Value "45.".
b = 45.
print(type(b))

# Assign the Variable "c" to the Float Value ".115".
c = .115
print(type(c))

# Assign the Variable "d" to the Float Value ".64e3".
d = .64e3
print(type(d))

# Assign the Variable "e" to the Float Value ".21E78".
e = .21E78
print(type(e))

# Assign the Variable "f" to the Float Value "33.6E-8".
f = 33.6E-8
print(type(f))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Complex Number

# COMMAND ----------

# Assign the Variable "a" to the Complex Number Value "6+3j".
a = 6+3j
print(type(a))

# Assign the Variable "b" to the Complex Number Value "52-81j".
b = 52-81j
print(type(b))

# Assign the Variable "c" to the Complex Number Value "-49+12j".
c = -49+12j
print(type(c))
