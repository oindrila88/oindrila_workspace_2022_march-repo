# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Generator Expressions"
# MAGIC * Topic: Introduction to "Generator Expressions" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What are "Generator Expressions"?
# MAGIC * The "<b>Generator Expressions</b>" are a "<b>Cross</b>" between the "<b>Python Comprehensions</b>", and, the "<b>Generator Functions</b>".
# MAGIC * The "<b>Generator Expressions</b>" use a "<b>Similar Syntax</b>" as the "<b>Python Comprehensions</b>", but, the "<b>Syntax</b>" "<b>Results In</b>" the "<b>Creation</b>" of a "<b>Generator Object</b>", which "<b>Produces</b>" the "<b>Specified Sequence</b>" "<b>Lazily</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to "Create" the "Generator Expressions"?
# MAGIC * The "<b>Syntax</b>" to "<b>Create</b>" the "<b>Generator Expressions</b>" is "<b>Very Similar</b>" to "<b>List Comprehension</b>", but, "<b>Delimited</b>" by "<b>Parenthesis</b>", i.e., "<b>()</b>", instead of the "<b>Square Brackets</b>", i.e., "<b>[]</b>" used for the "<b>List Comprehensions</b>".
# MAGIC * "<b>Generator Expressions</b>" are "<b>Useful</b>" for "<b>Situations</b>", where the "<b>Lazy Evaluation</b>" of the "<b>Generators</b>" with the "<b>Declarative Concision of Comprehensions</b>" is "<b>Required</b>".
# MAGIC * Example - the following "<b>Generator Expression</b>" "<b>Yields</b>" a "<b>List</b>" of the "<b>First 1 Million Square Numbers</b>"

# COMMAND ----------

# DBTITLE 1,"Create" a "Generator Expression" that "Yields" a "List" of the "First 1 Million Square Numbers"
genExpForFirstOneMillionSquareNum = (num * num for num in range(1, 1000001))

# COMMAND ----------

# MAGIC %md
# MAGIC * At this point, "<b>None</b>" of the "<b>Square Numbers</b>" have been "<b>Created</b>". Only the "<b>Specification</b>" of the "<b>Sequence</b>" is "<b>Captured</b>" into a "<b>Generator Object</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Perform</b>" a "<b>Force Evaluation</b>" on the "<b>Generator</b>" by using it to "<b>Create</b>" a "<b>Long List</b>".
# MAGIC * The "<b>Long List</b>" obviously "<b>Consumes</b>" a "<b>Significant Chunk of Memory</b>"

# COMMAND ----------

# DBTITLE 1,"Create" a "Long List" by "Calling" the "Generator Expression"
import sys

longList = list(genExpForFirstOneMillionSquareNum)[-10:]
print(longList)
print(sys.getsizeof(longList))

# COMMAND ----------

# MAGIC %md
# MAGIC * Since, the "<b>Generator Object</b>" is just an "<b>Iterator</b>", and, "<b>Once Run Exhaustively</b>" in the above way, will "<b>Yield No More Items</b>".
# MAGIC * Therefore, "<b>Repeating</b>" the "<b>Previous Statement</b>" of "<b>Force Evaluation</b>" on the "<b>Generator</b>" by using it to "<b>Create</b>" a "<b>Long List</b>" will "<b>Return</b>" an "<b>Empty List</b>".

# COMMAND ----------

# DBTITLE 1,This Time "Long List" by "Calling" the "Generator Expression" "Can't be Created"
longList = list(genExpForFirstOneMillionSquareNum)[-10:]
print(longList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Re-Create" a "Generator Object" from a "Generator Expression"
# MAGIC * "<b>Generators</b>" are "<b>Single-Use Objects</b>", meaning that "<b>Each Time</b>" a "<b>Generator Function</b>" is "<b>Called</b>", a "<b>New Generator Object</b>" is "<b>Created</b>".
# MAGIC * To "<b>Re-Create</b>" a "<b>Generator Object</b>" from a "<b>Generator Expression</b>", the "<b>Generator Expression</b>" itself must be "<b>Executed Once More</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Compute</b>" the "<b>Sum</b>" of the "<b>First 10 Million Square Numbers</b>" using the <b>Built-In Function</b> "<b>sum ()</b>", which "<b>Accepts</b>" an "<b>Iterable Series of Numbers</b>".
# MAGIC * If this were a "<b>List Comprehension</b>", the "<b>Memory Consumption</b>" would have been "<b>High</b>".
# MAGIC * Using the "<b>Generator Expression</b>", the "<b>Memory Consumption</b>" would be "<b>Insignificant</b>".
# MAGIC * The following example "<b>Produces</b>" the "<b>Result</b>" in a "<b>Second</b>", or, so, and, "<b>Uses Almost No Memory</b>".

# COMMAND ----------

# DBTITLE 1,"Compute" the "Sum" of the "First 10 Million Square Numbers" Using the "Generator Expression"
genExpForFirstOneMillionSquareNum = sum(num * num for num in range(1, 10000001))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Delimiting" a "Generator Expression" Using "Parenthesis" is "Optional"
# MAGIC * In the above example, it is seen that, "<b>No Separate Enclosing Parenthesis</b>", which are "<b>Needed</b>" "<b>In Addition</b>" to the <b>Function</b> "<b>sum ()</b>", is "<b>Supplied</b>" for the "<b>Generator Expression</b>".
# MAGIC * This "<b>Elegant Ability</b>" to have the "<b>Parenthesis</b>" used for the "<b>Function Call</b>" also "<b>Serve</b>" for the "<b>Generator Expression</b>" "<b>Aides</b>" the "<b>Readability</b>".

# COMMAND ----------

# DBTITLE 1,Use the "Second Set" of "Parenthesis" in the "Generator Expression"
genExpForFirstOneMillionSquareNum = (sum(num * num for num in range(1, 10000001)))

# COMMAND ----------

# MAGIC %md
# MAGIC # Conditional Generator Expression
# MAGIC * As with the "<b>Comprehensions</b>", an "<b>IF</b>" <b>Clause</b> can be also be used at the "<b>End</b>" of a "<b>Generator Expression</b>".

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
# MAGIC * In the following example, the <b>Function</b> "<b>isPrimeNumber</b>" can be "<b>Used</b>" to "<b>Determine</b>" the "<b>Sum</b>" of "<b>All</b>" the "<b>Integers</b>" between the "<b>Range</b>" of "<b>1 to 1000</b>".

# COMMAND ----------

# DBTITLE 1,"Determine" the "Sum" of "All" the "Integers" between the "Range" of "1 to 1000"
sumOfPrimesFrom1To1000 = sum(num * num for num in range(1, 1001) if isPrimeNumber(num))
print(sumOfPrimesFrom1To1000)
