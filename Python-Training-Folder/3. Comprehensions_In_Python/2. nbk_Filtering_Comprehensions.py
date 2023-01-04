# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Filtering Comprehension"
# MAGIC * Topic: Introduction to "Filtering" the "Comprehension" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Filtering Comprehension"?
# MAGIC * "<b>All</b>" the "<b>Three</b>" <b>Types</b> of "<b>Collection Comprehensions</b>" "<b>Support</b>" an "<b>Optional Filtering Clause</b>", i.e., "<b>if</b>" <b>Clause</b>.
# MAGIC * This "<b>Clause</b>" "<b>Allows</b>" to "<b>Choose</b>" "<b>Which Items</b>" of the "<b>Source</b>" are "<b>Evaluated</b>" by the "<b>Expression</b>" on the "<b>Left Collection</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Filtering Predicate Function" to "Check" If a "Number" is "Prime Number"
from math import sqrt

def isPrimeNumber (inputNum):
  # If the "Input Number" is "Less Than 2", then "Not Prime Number". So, "Return" "False".
  if inputNum < 2:
    return False
  # If the "Input Number" can be "Evenly Divided" by "Any Integer" Upto the "Square Root" of the "Input Number", then "Not Prime Number". So, "Return" "False".
  for num in range(2, int(sqrt(inputNum)) + 1):
    if inputNum % num == 0:
      return False
  # If "No Divisors" are "Found",  then "Prime Number". So, "Return" "True".
  return True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering List Comprehension
# MAGIC * In the following example, the <b>Function</b> "<b>isPrimeNumber</b>" can be "<b>Used</b>" as the "<b>Filtering Clause</b>" of the "<b>List Comprehension</b>" to "</b>Produce</b>" "<b>All</b>" the "<b>Prime Numbers</b>" that are "<b>Present</b>" in the "<b>Existing List</b>", i.e., "<b>listOfIntNumbers</b>".
# MAGIC * Since, in the following example, "<b>No Transformation</b>" is "<b>Applied</b>" to the "<b>Filtered Values</b>", the "<b>Expression</b>" in terms of "<b>num</b>" is simply "<b>num</b>" itself.

# COMMAND ----------

# DBTITLE 1,Create a "New List" From an "Existing List" Using "List Comprehension"
# Create a "List" of "Integers"
listOfIntNumbers = [25, 19, 15, 20, 1, 7, 0, 21, 29, 35, 55]

# Create a "New List" From "List" of "Integers" By "Filtering" with the "Predicate Function", i.e., "isPrimeNumber ()"
listOfPrimeIntNumbers = [intNum for intNum in listOfIntNumbers if isPrimeNumber(intNum)]
print(listOfPrimeIntNumbers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Set Comprehension
# MAGIC * In the following example, the <b>Function</b> "<b>isPrimeNumber</b>" can be "<b>Used</b>" as the "<b>Filtering Clause</b>" of the "<b>Set Comprehension</b>" to "</b>Produce</b>" "<b>All</b>" the "<b>Prime Numbers</b>" that are "<b>Present</b>" in the "<b>Existing Set</b>", i.e., "<b>setOfIntNumbers</b>".
# MAGIC * Since, in the following example, "<b>No Transformation</b>" is "<b>Applied</b>" to the "<b>Filtered Values</b>", the "<b>Expression</b>" in terms of "<b>num</b>" is simply "<b>num</b>" itself.

# COMMAND ----------

# DBTITLE 1,Create a "New Set" From an "Existing Set" Using "Set Comprehension"
# Create a "Set" of "Integers"
setOfIntNumbers = {25, 19, 15, 20, 1, 7, 0, 21, 29, 35, 55}

# Create a "New Set" From "Set" of "Integers" By "Filtering" with the "Predicate Function", i.e., "isPrimeNumber ()"
setOfPrimeIntNumbers = {intNum for intNum in setOfIntNumbers if isPrimeNumber(intNum)}
print(setOfPrimeIntNumbers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Dictionary Comprehension
# MAGIC * In the following example, the <b>Function</b> "<b>isSeniorCitizen</b>" can be "<b>Used</b>" as the "<b>Filtering Clause</b>" of the "<b>Dictionary Comprehension</b>" to "</b>Produce</b>" "<b>All</b>" the "<b>Persons</b>" with "<b>Age</b>" as "<b>Greater Than 60</b>" that are "<b>Present</b>" in the "<b>Existing Dictionary</b>", i.e., "<b>dictPerson</b>".
# MAGIC * Since, in the following example, "<b>No Transformation</b>" is "<b>Applied</b>" to the "<b>Filtered Values</b>", the "<b>Expression</b>" in terms of "<b>person</b>", and, "<b>age</b>" are simply "<b>person</b>", and, "<b>age</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Filtering Predicate Function" to "Check" If a "Person" is "Senior Citizen"
def isSeniorCitizen (age):
  if age >= 60:
    return True
  else:
    return False;

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
# Create a "Dictionary" Having Name" as the "Key", and, the Corresponding "Person's Age" as the "Value"
dictPerson = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

dictPersonWithSeniorCitizen = {person : age for person, age in dictPerson.items() if isSeniorCitizen(age)}
print(dictPersonWithSeniorCitizen)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Combine" the "Filtering Predicate Function" with the "Transformation Expression"
# MAGIC * It is possible to "<b>Combine</b>" the "<b>Filtering Predicate Function</b>" with the "<b>Transformation Expression</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
# Create a "Dictionary" Having Name" as the "Key", and, the Corresponding "Person's Age" as the "Value"
dictPerson = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

dictPersonWithSeniorCitizen = {person : isSeniorCitizen(age) for person, age in dictPerson.items() if isSeniorCitizen(age)}
print(dictPersonWithSeniorCitizen)
