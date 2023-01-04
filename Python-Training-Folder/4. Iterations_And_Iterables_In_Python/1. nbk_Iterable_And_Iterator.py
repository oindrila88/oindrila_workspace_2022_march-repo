# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Iterable" and "Iterator"
# MAGIC * Topic: Introduction to "Iterable" and "Iterator" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "Iterables" and "Iterators" are Required?
# MAGIC * "<b>Comprehensions</b>", and, "<b>for</b>" <b>Loops</b> are the "<b>Most Frequently Used Language Features</b>" for "<b>Performing</b>" the "<b>Iteration</b>". That is, "<b>Taking Elements One by One</b>" from a "<b>Source</b>", and, "<b>Doing Something</b>" with "<b>Each Element</b>" in turn.
# MAGIC * However, "<b>both</b>" the "<b>Comprehensions</b>", and, "<b>for</b>" <b>Loops</b> "<b>Iterate Over</b>" the "<b>Whole Sequence</b>" by default.
# MAGIC * When, "<b>More Fine-Grained Control</b>" is "<b>Needed</b>", there are "<b>Two Approaches</b>" -
# MAGIC   * <b>1</b>. <b>Iterable Objects</b> - The "<b>Iterable Protocol</b>" "<b>Allows</b>" to "<b>Pass</b>" an "<b>Iterable Object</b>", usually a "<b>Collection</b>", or, "<b>Stream of Objects</b>", such as a "<b>List</b>" to the <b>Built-In Function</b> "<b>iter ()</b>" to "<b>Get</b>" an "<b>Iterator</b>" for the "<b>Iterable Object</b>".
# MAGIC   * <b>2</b>. <b>Iterator Objects</b> - The "<b>Iterable Objects</b>", in turn, "<b>Support</b>" the "<b>Iterator Protocol</b>", which "<b>Requires</b>" to "<b>Pass</b>" the "<b>Iterator Object</b>" to the <b>Built-In Function</b> "<b>next ()</b>" to "<b>Fetch</b>" the "<b>Next Value</b>" from the "<b>Underlying Collection</b>".
# MAGIC   <br>"<b>Each Call</b>" to the <b>Built-In Function</b> "<b>next ()</b>" "<b>Moves</b>" the "<b>Iterator</b>" through the "<b>Underlying Collection</b>".

# COMMAND ----------

# DBTITLE 1,Create a "List" of "Strings"
# Create a "List" of "Strings" Containing "Names"
listOfNames = ["Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"]

# COMMAND ----------

# DBTITLE 1,Create an "Iterable Object" by "Passing" the "List of String" to the Function "iter ()"
iterableListOfNames = iter(listOfNames)

# COMMAND ----------

# DBTITLE 1,"Request" a "Value" from the "Iterable Object", i.e., "iterableListOfNames" Using the Function "next ()"
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))
print(next(iterableListOfNames))

# COMMAND ----------

# MAGIC %md
# MAGIC ### When the "Iterator" Reaches the "End"
# MAGIC * When the "<b>Iterator</b>" "<b>Reaches</b>" the "<b>End</b>" of the "<b>Underlying Collection</b>", "<b>Python</b>" "<b>Raises</b>" an "<b>Exception</b>", specifically of the <b>Type</b> "<b>StopIteration</b>".

# COMMAND ----------

# DBTITLE 1,What Happens When "Iterator" Reaches the "End"?
print(next(iterableListOfNames))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator"
# MAGIC * With "<b>for</b>" <b>Loops</b>, and, "<b>Comprehensions</b>" at the "<b>User's Fingertips</b>", the "<b>Utility</b>" of the "<b>Lower Level Iteration Protocols</b>" "<b>May Not</b>" be "<b>Obvious</b>".
# MAGIC * To "<b>Demonstrate</b>" "<b>More Concrete Use</b>", let's "<b>Create</b>" the following "<b>Utility Function</b>", which, when an "<b>Iterable Object</b>" is "<b>Passed</b>", "<b>Returns</b>" the "<b>First Item</b>" in the "<b>Underlying Collection</b>", or, "<b>If</b>" the "<b>Underlying Collection</b>" is "<b>Empty</b>", the "<b>Utility Function</b>" "<b>Raises</b>" a "<b>ValueError</b>" <b>Exception</b> -
# MAGIC   * First, "<b>Call</b>" the <b>Built-In Function</b> "<b>iter ()</b>" on the "<b>Input Iterable Object</b>" to "<b>Produce</b>" an "<b>Iterator</b>".
# MAGIC   * Second, the <b>Built-In Function</b> "<b>next ()</b>" is "<b>Called</b>" on the "<b>Iterator</b>" "<b>Inside</b>" a "<b>try</b>" <b>Block</b> that "<b>Returns</b>" the "<b>Result</b>".
# MAGIC   <br><b>If</b> the "<b>Input Iterable</b>" is "<b>Empty</b>", the <b>Built-In Function</b> "<b>next ()</b>" has "<b>No Value</b>" to "<b>Return</b>", and, it instead "<b>Raises</b>" a "<b>StopIteration</b>" <b>Exception</b>.
# MAGIC   <br>The "<b>StopIteration</b>" <b>Exception</b> is "<b>Caught</b>" in the "<b>except</b>" <b>Block</b>, and, a "<b>ValueError</b>" <b>Exception</b> is "<b>Raised</b>".
# MAGIC * This "<b>Utility Function</b>" "<b>Works as Expected</b>" on "<b>Any Iterable Object</b>".

# COMMAND ----------

# DBTITLE 1,Create the "Utility Function", i.e., "returnFirstElement" from "Any Iterable Object"
def returnFirstElement (iterableObj):
  iteratorObj = iter(iterableObj)
  
  try:
    return next(iteratorObj)
  except StopIteration:
    raise ValueError("Iterable Object is Empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator" with "String"

# COMMAND ----------

# DBTITLE 1,Pass a "String" to the "Utility Function", i.e., "returnFirstElement"
# Create a "String"
name = 'Oindrila Chakraborty'

print(returnFirstElement(name))

# COMMAND ----------

# DBTITLE 1,Pass an "Empty String" to the "Utility Function", i.e., "returnFirstElement"
# Create an "Empty String"
emptyString = ''

print(returnFirstElement(emptyString))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator" with "List"

# COMMAND ----------

# DBTITLE 1,Pass a "List of Integers" to the "Utility Function", i.e., "returnFirstElement"
# Create a "List" of "Integers"
listOfIntNumbers = [25, 19, 15, 20, 1, 7, 0, 21, 29, 35, 55]

print(returnFirstElement(listOfIntNumbers))

# COMMAND ----------

# DBTITLE 1,Pass an "Empty List" to the "Utility Function", i.e., "returnFirstElement"
# Create an "Empty List"
emptyList = []

print(returnFirstElement(emptyList))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator" with "Tuple"

# COMMAND ----------

# DBTITLE 1,Pass a "Tuple of Integers" to the "Utility Function", i.e., "returnFirstElement"
# Create a "Tuple" of "Integers"
tupleOfIntNumbers = (25, 19, 15, 20, 1, 7, 0, 21, 29, 35, 55)

print(returnFirstElement(tupleOfIntNumbers))

# COMMAND ----------

# DBTITLE 1,Pass an "Empty Tuple" to the "Utility Function", i.e., "returnFirstElement"
# Create an "Empty Tuple"
emptyTuple = ()

print(returnFirstElement(emptyTuple))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator" with "Set"

# COMMAND ----------

# DBTITLE 1,Pass a "Set of Integers" to the "Utility Function", i.e., "returnFirstElement"
# Create a "Set" of "Integers"
setOfIntNumbers = {25, 19, 15, 20, 1, 7, 0, 21, 29, 35, 55}

print(returnFirstElement(setOfIntNumbers))

# COMMAND ----------

# DBTITLE 1,Pass an "Empty Set" to the "Utility Function", i.e., "returnFirstElement"
# Create an "Empty Set"
emptySet = set()

print(returnFirstElement(emptySet))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear Example of "Iterable" and "Iterator" with "Dictionary"

# COMMAND ----------

# DBTITLE 1,Pass a "Dictionary" to the "Utility Function", i.e., "returnFirstElement"
# Create a "Dictionary" Having Name" as the "Key", and, the Corresponding "Person's Age" as the "Value"
dictPerson = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(returnFirstElement(dictPerson))

# COMMAND ----------

# DBTITLE 1,Pass an "Empty Dictionary" to the "Utility Function", i.e., "returnFirstElement"
# Create an "Empty Dictionary"
emptyDictionary = {}

print(returnFirstElement(emptyDictionary))

# COMMAND ----------

# MAGIC %md
# MAGIC * It is worth noting that the "<b>Higher-Level Iteration Constructions</b>", such as "<b>for</b>" <b>Loops</b>, and, "<b>Comprehensions</b>" are "<b>Built Directly</b>" upon the "<b>Lower-Level Iteration Protocol</b>".
