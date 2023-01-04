# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Range"
# MAGIC * Topic: Introduction to the "Range" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Range"?
# MAGIC * A "<b>Range</b>" is a "<b>Collection</b>", rather than a "<b>Container</b>".
# MAGIC * A "<b>Range</b>" is a "<b>Type</b>" of "<b>Sequence</b>" that is used to "<b>Represent</b>" an "<b>Arithmatic Progression</b>" of "<b>Integers</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Ranges" are "Created"?
# MAGIC * "<b>Ranges</b>" are "<b>Created</b>" by "<b>Calling</b>" the "<b>range () Constructor</b>".
# MAGIC * There is "<b>No Literal Form</b>" to "<b>Create</b>" a "<b>Range</b>".
# MAGIC * Most typically, when "<b>Creating</b>" a "<b>Range</b>" by "<b>Calling</b>" the "<b>range () Constructor</b>", "<b>Only</b>" the "<b>Stop Value</b>" of the "<b>Range</b>" to "<b>Create</b>" is "<b>Provided</b>", and, "<b>Python</b>" assumes the "<b>Starting Value</b>" to be "<b>0</b>", by default.
# MAGIC * It is possible to "<b>Supply</b>" a "<b>Starting Value</b>" as well, by "<b>Passing</b>" the "<b>Two Arguments</b>" to the "<b>range () Constructor</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Range" by Providing Only the "Stop Value" to the "range ()" Constructor
range(10)

# COMMAND ----------

# DBTITLE 1,Create a "Range" by Providing Both the "Starting Value" and "Stop Value" to the "range ()" Constructor
range(10, 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Half-Open Range Convention
# MAGIC * "<b>Ranges</b>" are sometimes used to "<b>Create</b>" the "<b>Consecutive Integers</b>" to be used as "<b>Loop Counters</b>".
# MAGIC * In this case, the "<b>Stop Value</b>", which is "<b>Supplied</b>" to the "<b>range () Constructor</b>", is "<b>1 Past</b>" the "<b>End</b>" of the "<b>Sequence</b>", and, the "<b>Stop Value</b>" of the "<b>Range</b>" is "<b>Not Displayed</b>" using the "<b>Loop</b>".
# MAGIC * The situation, where the "<b>Stop Value</b>" is "<b>Not Included</b>" in the "<b>Sequence</b>" of a "<b>Range</b>", is called as the "<b>Half-Open Range Convention</b>".

# COMMAND ----------

# DBTITLE 1,Display the "Consecutive Integers" of a "Range", Defined With Only "Stop Value", Using "Loop"
for num in range(10):
  print(num)

# COMMAND ----------

# DBTITLE 1,Display the "Consecutive Integers" of a "Range", Defined With Both "Starting Value" and "Stop Value", Using "Loop"
for num in range(10, 20):
  print(num)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Insight" into the "Half-Open Range Convention" Situation
# MAGIC * The "<b>Half-Open Range Convention</b>" situation, i.e., the "<b>Stop Value</b>" "<b>Not Being Included</b>" in the "<b>Sequence</b>" of a "<b>Range</b>" may seem strange at first, but it actually makes a lot of sense when dealing with "<b>Consecutive Ranges</b>", because, the "<b>Stop Value</b>", specified by "<b>One Range</b>" is the "<bStarting Value</b>" of the "<b>Netx Range</b>".
# MAGIC * "<b>Wrapping</b>" the "<b>Call</b>" to the "<b>range () Constructor</b>" inside another "<b>Call</b>" to the "<b>list () Constructor</b>" is a handy way to "<b>Force Production of Each Item</b>".

# COMMAND ----------

# DBTITLE 1,Call "range () Constructor" From Within "list () Constructor"
print(list(range(5)))
print(list(range(5, 10)))
print(list(range(10, 15)))
print(list(range(15, 20)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create "Range" With "Step Argument"
# MAGIC * The "<b>range () Constructor</b>" also supports a "<b>Step Argument</b>".
# MAGIC * In order to use the "<b>Step Argument</b>" in the "<b>range () Constructor</b>", it is "<b>Mandatory</b>" to "<b>Supply All Three Arguments</b>" to the "<b>range () Constructor</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Range" With "Step Argument"
print(list(range(0, 10, 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Insight" Into the "range () Constructor"
# MAGIC * "<b>Range</b>" is curious in that it determines what its "<b>Arguments</b>" mean by "<b>Counting</b>" the "<b>Number of Arguments</b>" -
# MAGIC <br><b>1</b>. Providing "<b>Only One Argument</b>" means that the "<b>Argument</b>" is the "<b>Stop Value</b>".
# MAGIC <br><b>2</b>. Providing "<b>Two Arguments</b>" mean that the "<b>Arguments</b>" are the "<b>Starting Value</b>", and, the "<b>Stop Value</b>" respectively.
# MAGIC <br><b>3</b>. Providing "<b>Three Arguments</b>" mean that the "<b>Arguments</b>" are the "<b>Starting Value</b>", the "<b>Stop Value</b>", and, the "<b>Step Value</b>" respectively.
# MAGIC * "<b>Python Range</b>" works in such a way that the "<b>First Argument</b>", i.e., the "<b>Starting Value</b>" is made "<b>Optional</b>".
# MAGIC * "<b>Python Range</b>" Does "<b>Not Support</b>" the "<b>Keyword Arguments</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Poorly Styled Written Code With "Python Range"
# MAGIC * Following type of "<b>Code Should be Avoided</b>" -
# MAGIC * Example - A "<b>Poor Way</b>" of "<b>Coding</b>" to "<b>Print</b>" the "<b>Elements</b>" in a "<b>List</b>" is by "<b>Constructing</b>" a "<b>Range</b>" over the "<b>Length</b>" of the "<b>List</b>", and, then "<b>Indexing</b>" into the "<b>List</b>" on "<b>Each Iteration</b>".

# COMMAND ----------

# DBTITLE 1,"Poor Way" of "Coding" to "Print" the "Elements" in a "List"
myList = [25, 10, 5, 20, 15]

for num in range(len(myList)):
  print(myList[num])

# COMMAND ----------

# MAGIC %md
# MAGIC * Although the above "<b>Style of Code Works</b>", but, it "<b>Should Not be Used</b>".
# MAGIC * Instead, it is "<b>Always Preferable</b>" to use "<b>Iteration</b>" over the "<b>Objects</b>".

# COMMAND ----------

# DBTITLE 1,"Correct Way" of "Coding" to "Print" the "Elements" in a "List"
myList = [25, 10, 5, 20, 15]

for num in myList:
  print(num)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Enumerate" in "Python"
# MAGIC * If a "<b>Counter</b>" is needed on "<b>Another Iterable Object</b>", then the "<b>Python Built-In Function</b>", i.e., "<b>enumerate</b>" can be used.
# MAGIC * The "<b>enumerate</b>" Function "<b>Returns</b>" an "<b>Iterable Series of Pairs</b>", where each "<b>Pair</b>" is a "<b>Tuple</b>".
# MAGIC * The "<b>First Element</b>" of the "<b>Pair</b>" is the "<b>Index</b>" of the "<b>Current Item</b>".
# MAGIC * The "<b>Second Element</b>" of the "<b>Pair</b>" is the "<b>Item</b>" itself.

# COMMAND ----------

# DBTITLE 1,Display "All" the "Items" of "List" Along With the "Corresponding Index Positions" in the "List" Using "enumerator" Function
myList = [25, 10, 5, 20, 15]

for num in enumerate(myList):
  print(num)

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to use the "<b>Tuple Unpacking</b>" to "<b>Avoid Dealing</b>" with the "<b>Tuples Directly</b>".

# COMMAND ----------

# DBTITLE 1,Display "All" the "Items" of "List" Along With the "Corresponding Index Positions" in the "List" Using "enumerator" Function With "Tuple Unpacking"
myList = [25, 10, 5, 20, 15]

for pos, num in enumerate(myList):
  print(f"Index Position: {pos}, Number: {num}")
