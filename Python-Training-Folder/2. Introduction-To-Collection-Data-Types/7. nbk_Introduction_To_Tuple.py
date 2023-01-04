# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Tuple"
# MAGIC * Topic: Introduction to the "Tuple" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Tuple"?
# MAGIC * "<b>Python Tuples</b>" are the "<b>Immutable Sequences of Arbitrary Objects</b>".
# MAGIC * Once "<b>Created</b>", the "<b>Elements</b>" within the "<b>Python Tuples</b>" "<b>Cannot</b>" be "<b>Replaced</b>" or "<b>Removed</b>", and, "<b>New Elements</b>" "<b>Cannot</b>" be "<b>Added</b>" to the "<b>Python Tuples</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Python Tuples" are "Created"?
# MAGIC * "<b>Python Tuples</b>" are "<b>Delimited</b>" by "<b>Parentheses</b>", i.e., "<b>()</b>".
# MAGIC * The "<b>Items</b>" within the "<b>Tuples</b>" are "<b>Separated</b>" by "<b>Commas</b>", i.e., "<b>,</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Tuple" of "Integers"
(25, 10, 5, 20, 15)

# COMMAND ----------

# DBTITLE 1,Create a "Python Tuple" of "Strings"
('Oindrila', "Soumyajyoti", 'Kasturi', "Rama")

# COMMAND ----------

# MAGIC %md
# MAGIC * In many cases, the "<b>Parentheses</b>" of a "<b>Literal Tuple</b>" may be "<b>Omitted</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Tuple" of "Integers" Without the "Parentheses"
25, 10, 5, 20, 15

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>Feature</b>" of "<b>Not Using</b>" the "<b>Parentheses</b>" is often used when "<b>Returning Multiple Values</b>" from a "<b>Function</b>".

# COMMAND ----------

# DBTITLE 1,"Create" and "Call" a "Function" to "Return" the "Max" and "Min" Values of a "Sequence"
def getMaxAndMin (elements):
  return max(elements), min(elements)

print(getMaxAndMin([25, 10, 5, 20, 15]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Heterogeneous Tuple"
# MAGIC * "<b>Python Tuples</b>" can be "<b>Heterogeneous</b>". That means "<b>Each</b>" of the "<b>Elements</b>" inside a "<b>Python Tuple</b>" can be of "<b>Different Data Types</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Heterogeneous Python Tuple"
(25, 'Oindrila', False, 30.456, 'Kasturi', True, 15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an "Empty Tuple"
# MAGIC * It is possible to "<b>Create</b>" an "<b>Empty Tuple</b>" by using an "<b>Empty Parentheses</b>".

# COMMAND ----------

# DBTITLE 1,Create an "Empty Python Tuple"
()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Tuple" From "Other Collection Data Types"
# MAGIC * "<b>Python Tuples</b>" can be "<b>Created</b>" from "<b>Other Collection Data Types</b>", such as - "<b>Strings</b>", using the "<b>tuple () Constructor</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Tuple" From a "String"
tuple("Oindrila")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Tuple" With "Single Element"
# MAGIC * It is "<b>Not Possible</b>" to "<b>Create</b>" a "<b>Tuple</b>" with "<b>Single Element</b>" by providing a "<b>Simple Number</b>" in "<b>Parentheses</b>".
# MAGIC * This is because "<b>Python</b>" "<b>Parses</b>" a "<b>Simple Number</b>" in "<b>Parentheses</b>" as an "<b>Integer</b>" that is "<b>Enclosed</b>" in the "<b>Precedence Controlling Parentheses</b>" of a "<b>Math Expression</b>".

# COMMAND ----------

# DBTITLE 1,This is "Not" the "Right Way" to "Create" a "Tuple" with "Single Element"
singleIntTuple = (4)
print(type(singleIntTuple))

# COMMAND ----------

# MAGIC %md
# MAGIC * To "<b>Create</b>" a "<b>Tuple</b>" with "<b>Single Element</b>", the "<b>Trailing Comma Separator</b>", i.e., "<b>,</b>" is used.
# MAGIC * A "<b>Single Element</b>" with a "<b>Trailing Comma</b>" is "<b>Parsed</b>" as a "<b>Tuple</b>" with "<b>Single Element</b>" in "<b>Python</b>".

# COMMAND ----------

# DBTITLE 1,The "Right Way" to "Create" a "Tuple" with "Single Element"
singleIntTuple = (4,)
print(type(singleIntTuple))

# COMMAND ----------

# MAGIC %md
# MAGIC # Tuple Unpacking
# MAGIC * "<b>Returning Multiple Values</b>" as a "<b>Tuple</b>" is often used in conjunction with a wonderful "<b>Feature</b>" of "<b>Python</b>", called "<b>Tuple Unpacking</b>".
# MAGIC * "<b>Tuple Unpacking</b>" is a "<b>De-Structuring Operation</b>", which allows to "<b>Unpack</b>" the "<b>Data Structures</b>" into the "<b>Named References</b>".

# COMMAND ----------

# DBTITLE 1,"Create" and "Call" a "Function" to "Return" the "Max" and "Min" Values of a "Sequence"
def getMaxAndMin (elements):
  return max(elements), min(elements)

# "Assign" the "Result" of the "getMaxAndMin ()" Function to "Two New References"
maxValue, minValue = getMaxAndMin([25, 10, 5, 20, 15])
print(maxValue)
print(minValue)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Retrieve Elements" From a "Tuple"
# MAGIC * It is possible to "<b>Retrieve</b>" the "<b>Elements</b>" of a "<b>Tuple</b>" by using the "<b>Square Brackets</b>", i.e., "<b>[]</b>", with a "<b>Zero-Based Index</b>".
# MAGIC * Example - To "<b>Retrieve</b>" the "<b>First Element</b>" of a "<b>Tuple</b>", "<b>[0]</b>" should be used, to "<b>Retrieve</b>" the "<b>Second Element</b>" of a "<b>Tuple</b>", "<b>[1]</b>" should be used, and so on.

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Third Element" of a "Literal Tuple"
intTuple = (25, 10, 5, 20, 15)
intTuple[2]

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Tuple Index</b>" of the "<b>Element</b>" to be "<b>Retrieved</b>" from a "<b>Tuple</b>" is "<b>Not Present</b>" in the "<b>Tuple</b>", the "<b>IndexError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Fifteenth Element", Present at "Index = 14", of a "Literal Tuple"
intTuple = (25, 10, 5, 20, 15)
intTuple[14]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Count" the "Number of Elements" of a "Python Tuple"
# MAGIC * It is possible to "<b>Determine</b>" the "<b>Number of Elements</b>" in a "<b>Tuple</b>" using the "<b>Python Built-In Function</b>", i.e., "<b>len ()</b>".

# COMMAND ----------

# DBTITLE 1,"Count" the "Number of Elements" of a "Python Tuple"
intTuple = (25, 10, 5, 20, 15)
len(intTuple)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Iterating Over" the "Elements" of "Python Tuple"
# MAGIC * Since "<b>Python Tuples</b>" are "<b>Iterables</b>", the "<b>for Loop</b>" can be used to "<b>Iterate Over</b>" the "<b>Elements</b>" of "<b>Python Tuples</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Elements" of a "Python Tuple" Using the "for Loop"
intTuple = (25, 10, 5, 20, 15)

for eachInt in intTuple:
  print(eachInt)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Concatenate" the "Tuples"
# MAGIC * There are "<b>Multiple Techniques</b> available to "<b>Concatenate</b>" the "<b>Tuples</b>".
# MAGIC * All of the "<b>Techniques</b>" will work with "<b>Any Iterable Series</b>" on the "<b>Right-Hand Side</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Concatenation" of the "Tuples" Using the "Plus Operator"
# MAGIC * It is possible to "<b>Concatenate</b>" the "<b>Tuples</b>" using the "<b>Plus Operator</b>", i.e., "<b>+</b>". This "<b>Results</b>" in a "<b>New Tuple</b>", "<b>Without Modification</b>" of the "<b>Tuples</b>" that are "<b>Concatenated</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Tuples" Using the "Plus Operator", i.e., "+"
myIntTuple = (25, 10, 5, 20, 15)
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")

newTuple = myIntTuple + myStrTuple
newTuple

# COMMAND ----------

# MAGIC %md
# MAGIC # "Usage" of "Multiplication Operator" on "Tuple"
# MAGIC * As with "<b>Strings</b>" and "<b>Lists</b>", the "<b>Tuples</b>" support "<b>Repetition</b>" using the "<b>Multiplication Operator</b>".
# MAGIC * The "<b>Multiplication Operator</b>" is often used for "<b>Initializing</b>" a "<b>Tuple</b>" of a "<b>Size</b>, <b>Known in Advance</b>" with a "<b>Constant Value</b>" for the "<b>Repetition</b>".

# COMMAND ----------

# DBTITLE 1,"Initialize" a "Tuple" of "Multiple Elements" Using "Multiplication Operator"
intTuple = (25, 10, 5, 20, 15)
newIntTuple = intTuple * 5
print(newIntTuple)

# COMMAND ----------

# DBTITLE 1,"Initialize" a "Tuple" of "Single Element" Using "Multiplication Operator"
intTuple = (2,)
newIntTuple = intTuple * 10
print(newIntTuple)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Search" the "Elements" in a "Tuple"
# MAGIC * There are "<b>Multiple Methods</b> available to "<b>Find</b>" the "<b>Elements</b>" in a "<b>Tuple</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Search" the "Elements" in a "Tuple" Using "index ()" Method
# MAGIC * The "<b>index ()</b>" Method "<b>Takes</b>" the "<b>Element</b>" to be "<b>Searched</b>" as the "<b>Argument</b>".
# MAGIC * The "<b>Elements</b>" present in "<b>Each Index</b>" of the "<b>Tuple</b>" are "<b>Compared</b>" with the "<b>Element</b>" to be "<b>Searched</b>" for "<b>Equivalence</b>" until the "<b>Element</b>" to be "<b>Searched</b>" is "<b>Found</b>".
# MAGIC * The "<b>index ()</b>" Method "<b>Returns</b>" the "<b>Index</b>" of the "<b>First Tuple Element</b>", which is "<b>Equal</b>" to the "<b>Argument</b>" sent to the "<b>index ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Find" the "First Occurrence" of an "Element" in a "Tuple" Using the "index ()" Method
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print(myStrTuple.index("Banana"))

# COMMAND ----------

# MAGIC %md
# MAGIC * If an "<b>Element</b>" is "<b>Searched</b>" that "<b>Does Not Exist</b>" in the "<b>Tuple</b>" using the "<b>index ()</b>" Method, the "<b>ValueError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Find" an "Element" That "Does Not Exist" in a "Tuple" Using the "index ()" Method - "ValueError"
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print(myStrTuple.index("Papaya"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Count" the "Number of Times" an "Element" "Appears" in a "Tuple" Using "count ()" Method
# MAGIC * The "<b>count ()</b>" Method "<b>Takes</b>" the "<b>Element</b>" to be "<b>Searched</b>" as the "<b>Argument</b>".
# MAGIC * The "<b>count ()</b>" Method "<b>Returns</b>" the "<b>Number of Times</b>" the "<b>Argument</b>", sent to the "<b>count ()</b>" Method, has "<b>Appeared</b>" in the "<b>Tuple</b>".

# COMMAND ----------

# DBTITLE 1,"Find" the "Number of Times" an "Element" is "Present" in the "Tuple" Using the "count ()" Method
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print(myStrTuple.count("Apple"))

# COMMAND ----------

# MAGIC %md
# MAGIC * If an "<b>Element</b>" is "<b>Searched</b>" that "<b>Does Not Exist</b>" in the "<b>Tuple</b>" using the "<b>count ()</b>" Method, "<b>No Exception</b>" is "<b>Thrown</b>". Instead "<b>0</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Find" the "Number of Times" an "Element" is "Present" in the "Tuple", which "Does Not Exist" in the "Tuple", Using the "count ()" Method
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print(myStrTuple.count("Papaya"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Membership Verification" of the "Elements" in a "Tuple"
# MAGIC * "<b>Membership</b>" is a "<b>Fundamental Operation</b>" for "<b>Tuples</b>".
# MAGIC * As with "<b>Other Collection Data Types</b>", the "<b>Membership Operation</b>" is "<b>Performed</b>" using the "<b>IN</b>" and "<b>NOT IN</b>" Operators on "<b>Tuples</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "Tuple" Using "IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Present</b>" in a "<b>Tuple</b>" using "<b>Membership</b>", then "<b>IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>Tuple</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>Tuple</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Present" in a "Tuple" Using the "IN Operator"
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print("Pomengrate" in myStrTuple)

print("Papaya" in myStrTuple)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "Tuple" Using "NOT IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Not Present</b>" in a "<b>Tuple</b>" using "<b>Membership</b>", then "<b>NOT IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>Tuple</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>Tuple</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Not Present" in a "Tuple" Using the "NOT IN Operator"
myStrTuple = ("Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple")
print("Papaya" not in myStrTuple)

print("Pomengrate" not in myStrTuple)
