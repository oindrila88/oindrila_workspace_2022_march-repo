# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Counter" Class
# MAGIC * Topic: Introduction to the "Counter" Class that is Used to "Extend" the "Built-In Collection Data Types", Offered by the "Python Standard Library"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Counter" Class?
# MAGIC * The "<b>Counter</b>" Class is an "<b>Extension</b>" of the "<b>Dictionary</b>" to "<b>Keep Track</b>" of the "<b>Number of Occurrences</b>" of a "<b>Value</b>".
# MAGIC * "<b>Counter</b>" Class is a "<b>Sub-Class</b>" of the "<b>Dict</b>" Class.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create an "Instance" of the "Counter" Class
# MAGIC * An "<b>Instance</b>" of the "<b>Counter</b>" Class can be "<b>Created</b>" just like any other "<b>Data Type</b>" in "<b>Python</b>".
# MAGIC * The "<b>Result</b>" will be an "<b>Object</b>" that "<b>Behaves</b>" much like a "<b>Dictionary</b>" with "<b>Keys</b>" and the "<b>Associated Values</b>". These are called "<b>Elements</b>" and "<b>Counts</b>" respectively in a "<b>Counter</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Initialize" an "Instance" of the "Counter" Class
# MAGIC * There are several ways to "<b>Initialize</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class with "<b>Counts</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize an Instance of the Counter with Nothing
# MAGIC * It is possible to "<b>Initialize</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class by "<b>Passing Nothing</b>" to the "<b>Initializer</b>" of the "<b>Counter</b>" Class.
# MAGIC <br>It will create a "<b>Counter</b>" with "<b>No Elements</b>", and, thus "<b>No Counts</b>".

# COMMAND ----------

# DBTITLE 1,Create an "Empty Counter"
from collections import Counter

emptyCounter = Counter()
print(emptyCounter)
print(type(emptyCounter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize an Instance of the Counter with a Dictionary
# MAGIC * It is possible to "<b>Initialize</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class by "<b>Passing</b>" a "<b>Dictionary</b>" to the "<b>Initializer</b>" of the "<b>Counter</b>" Class.
# MAGIC <br>In this case, the "<b>Keys</b>" in the "<b>Dictionary</b>" will become the "<b>Elements</b>" in the "<b>Counter</b>", and, the "<b>Associated Values</b>" will become the "<b>Counts</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Counter" from a "Dictionary"
from collections import Counter

dictFruits = {
  "Mango": 30,
  "Apple": 100,
  "Banana": 85,
  "Mango": 26,
  "Guava": 97,
  "Watermelon": 102,
  "Banana": 79
}

dictCounter = Counter(dictFruits)
print(dictCounter)
print(type(dictCounter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize an Instance of the Counter with a Python List
# MAGIC * It is possible to "<b>Initialize</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class by "<b>Passing</b>" a "<b>Python List</b>" to the "<b>Initializer</b>" of the "<b>Counter</b>" Class.
# MAGIC <br>In this case, the "<b>Unique Values</b>" in the "<b>Python List</b>" will become the "<b>Elements</b>" in the "<b>Counter</b>", and, the "<b>Number of Times Each Element is Repeated in the Python List</b>" will become the "<b>Counts</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Counter" from a "Python List"
from collections import Counter

numList = [20, 43, -6, 20, 78, 0, 12, 87, 2, 88, 2, 20, 34, 53]

listCounter = Counter(numList)
print(listCounter)
print(type(listCounter))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize an Instance of the Counter with Keyword Arguments
# MAGIC * It is possible to "<b>Initialize</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class by "<b>Passing</b>" a "<b>Set of Keyword Arguments</b>", using the syntax "<b>element = count</b>", to the "<b>Initializer</b>" of the "<b>Counter</b>" Class.

# COMMAND ----------

# DBTITLE 1,Create a "Counter" using "Keyword Arguments"
from collections import Counter

# "Keywords Cannot Be Repeated". If Repeated, the "SyntaxError" will be thrown.
# keywordArgumentCounter = Counter(Mango = 30, Apple = 100, Banana = 85, Mango = 26, Guava = 97, Watermelon = 102, Banana = 79)

keywordArgumentCounter = Counter(Mango = 30, Apple = 100, Banana = 85, Guava = 97, Watermelon = 102)
print(keywordArgumentCounter)
print(type(keywordArgumentCounter))

# COMMAND ----------

# MAGIC %md
# MAGIC # Useful "Methods" of the "Counter" Class
# MAGIC * After an "<b>Instance</b>" of the "<b>Counter</b>" Class is "<b>Initialized</b>", it is possible to "<b>Add</b>", or, "<b>Set Counts</b>" to the "<b>Instance</b>" of the "<b>Counter</b>" Class.

# COMMAND ----------

# MAGIC %md
# MAGIC ### "most_common" Method
# MAGIC * The "<b>most_common</b>" Method "<b>Returns</b>" a "<b>List of Tuples</b>".
# MAGIC * The "<b>First Value</b>" in the "<b>Tuple</b>" is an "<b>Element</b>" in the "<b>Counter</b>".
# MAGIC * The "<b>Second Value</b>" in the "<b>Tuple</b>" is the "<b>Associated Count</b>".
# MAGIC * The "<b>List of Tuples</b>" is "<b>Sorted</b>" in the "<b>Descending Order</b>" by "<b>Count</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC #### Limit the "Result" of the "most_common" Method
# MAGIC * The "<b>most_common</b>" Method also takes an "<b>Optional Integer</b>", which will "<b>Limit</b>" the "<b>Number of Results</b>" to the "<b>Number</b>" that is "<b>Passed In</b>".

# COMMAND ----------

# DBTITLE 1,Display Only the "Top Three Most Common Elements" and "Associated Counts"
from collections import Counter

numList = [20, 43, -6, 20, 78, 0, 12, 87, 2, 88, 2, 20, 34, 53]

listCounter = Counter(numList)
print(listCounter.most_common(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### When "Elements" with "Equal Counts" are "Encountered" in the "most_common" Method
# MAGIC * When "<b>Elements</b>" with "<b>Equal Counts</b>" are "<b>Encountered</b>" in the "<b>most_common</b>" Method, the "<b>Elements</b>" will be "<b>Displayed</b>" in the "<b>Order</b>" of "<b>First Encounter</b>" in the "<b>Counter</b>".

# COMMAND ----------

# DBTITLE 1,When "Elements" with "Equal Counts" are "Encountered"
from collections import Counter

numList = [20, 43, -6, 25, 78, 0, 12, 87, 2, 88, 2, 20, 34, 53]

listCounter = Counter(numList)
print(listCounter.most_common())

# COMMAND ----------

# MAGIC %md
# MAGIC ### "elements" Method
# MAGIC * The "<b>elements</b>" Method "<b>Expands</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class into a a "<b>List</b>".
# MAGIC * Each "<b>Element</b>" in the "<b>Counter</b>" will be "<b>Repeated</b>" in the "<b>List</b>" "<b>As Many Times</b>" as its "<b>Associated Count<b>".

# COMMAND ----------

# DBTITLE 1,Display the "Elements" of a "Counter" as a "List"
from collections import Counter

keywordArgumentCounter = Counter(Mango = 4, Apple = 10, Banana = 8, Guava = 6, Watermelon = 4)
counterList = keywordArgumentCounter.elements()
for counterValue in counterList:
  print(counterValue)

# COMMAND ----------

# MAGIC %md
# MAGIC #### "Element" Having "Count Lesser Than 1"
# MAGIC * "<b>Elements</b>", which have a "<b>Count</b>" of "<b>Less Than One</b>", are "<b>Omitted</b>" by the "<b>elements</b>" Method.

# COMMAND ----------

# DBTITLE 1,When "Element" Has "Count Lesser Than 1"
from collections import Counter

keywordArgumentCounter = Counter(Mango = 4, Apple = 10, Banana = 0, Guava = 6, Watermelon = 4)
counterList = keywordArgumentCounter.elements()
for counterValue in counterList:
  print(counterValue)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "update" Method
# MAGIC * The "<b>update</b>" Method "<b>Takes</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class, and, then "<b>Performs</b>" the "<b>Summation</b>" of the "<b>Counter</b>" of any "<b>Element</b>" that "<b>Exists</b>" in both the "<b>Instances</b>" of the "<b>Counter</b>" Class.
# MAGIC * If the "<b>Element</b>" "<b>Does Not Exists</b>" in the "<b>Calling Counter Instance</b>", then it is "<b>Added</b>" in the "<b>Calling Counter Instance</b>", whereas, in the "<b>update</b>" Method of the "<b>dict</b>" Class, the "<b>Already Existing Keys</b>" are "<b>Replaced</b>", instead of "<b>Adding</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "subtract" Method
# MAGIC * The "<b>subtract</b>" Method "<b>Takes</b>" an "<b>Instance</b>" of the "<b>Counter</b>" Class, and, then "<b>Performs</b>" the "<b>Subtraction</b>" of any "<b>Element</b>" that "<b>Exists</b>" in both the "<b>Instances</b>" of the "<b>Counter</b>" Class.
# MAGIC * If the "<b>Element</b>" "<b>Exists</b>" in the "<b>Calling Counter</b>", then it is "<b>Subtracted</b>" in the "<b>Calling Counter</b>".
