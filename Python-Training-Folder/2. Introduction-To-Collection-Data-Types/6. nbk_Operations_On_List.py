# Databricks notebook source
# MAGIC %md
# MAGIC # Operations On "List"
# MAGIC * Topic: Different "Operations" that Can be "Performed" on the "List" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Usage" of "Multiplication Operator" on "List"
# MAGIC * As with "<b>Strings</b>" and "<b>Tuples</b>", the "<b>Lists</b>" support "<b>Repetition</b>" using the "<b>Multiplication Operator</b>".
# MAGIC * The "<b>Multiplication Operator</b>" is often used for "<b>Initializing</b>" a "<b>List</b>" of a "<b>Size</b>, <b>Known in Advance</b>" with a "<b>Constant Value</b>" for the "<b>Repetition</b>".

# COMMAND ----------

# DBTITLE 1,"Initialize" a "List" of "Multiple Elements" Using "Multiplication Operator"
intList = [25, 10, 5, 20, 15]
newIntList = intList * 5
print(newIntList)

# COMMAND ----------

# DBTITLE 1,"Initialize" a "List" of "Single Element" Using "Multiplication Operator"
intList = [2]
newIntList = intList * 10
print(newIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC * When "<b>Mutable Object</b>", such as "<b>List</b>" itself, is used as the "<b>List Element</b>", the "<b>Multiplication Operator</b>" will "<b>Repeat</b>" the "<b>Reference</b>" to the "<b>Object</b>" in "<b>Each Index</b>" of the "<b>Original List Reference</b>", "<b>Without Copying</b>" the "<b>Actual Object</b>" itself in "<b>Each Index</b>".
# MAGIC * If "<b>Any Element</b>" from the "<b>Inner List</b>" is "<b>Modified</b>", the "<b>Change</b>" can be "<b>Reflected</b>" through "<b>All the Other References</b>", which "<b>Refer</b>" to the "<b>Same Changed Object</b>" in "<b>Other Inner List Elements</b>" as well.

# COMMAND ----------

# DBTITLE 1,"Modify" the "Third Nested List Element". "Change" Will "Reflect" in "Others"
myIntList = [[-2, +2]] * 5
print(myIntList)

myIntList[2].append(9)
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Search" the "Elements" in a "List"
# MAGIC * There are "<b>Multiple Methods</b> available to "<b>Find</b>" the "<b>Elements</b>" in a "<b>List</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Search" the "Elements" in a "List" Using "index ()" Method
# MAGIC * The "<b>index ()</b>" Method "<b>Takes</b>" the "<b>Element</b>" to be "<b>Searched</b>" as the "<b>Argument</b>".
# MAGIC * The "<b>Elements</b>" present in "<b>Each Index</b>" of the "<b>List</b>" are "<b>Compared</b>" with the "<b>Element</b>" to be "<b>Searched</b>" for "<b>Equivalence</b>" until the "<b>Element</b>" to be "<b>Searched</b>" is "<b>Found</b>".
# MAGIC * The "<b>index ()</b>" Method "<b>Returns</b>" the "<b>Index</b>" of the "<b>First List Element</b>", which is "<b>Equal</b>" to the "<b>Argument</b>" sent to the "<b>index ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Find" the "First Occurrence" of an "Element" in a "List" Using the "index ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print(myStrList.index("Banana"))

# COMMAND ----------

# MAGIC %md
# MAGIC * If an "<b>Element</b>" is "<b>Searched</b>" that "<b>Does Not Exist</b>" in the "<b>List</b>" using the "<b>index ()</b>" Method, the "<b>ValueError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Find" an "Element" That "Does Not Exist" in a "List" Using the "index ()" Method - "ValueError"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print(myStrList.index("Papaya"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Count" the "Number of Times" an "Element" "Appears" in a "List" Using "count ()" Method
# MAGIC * The "<b>count ()</b>" Method "<b>Takes</b>" the "<b>Element</b>" to be "<b>Searched</b>" as the "<b>Argument</b>".
# MAGIC * The "<b>count ()</b>" Method "<b>Returns</b>" the "<b>Number of Times</b>" the "<b>Argument</b>", sent to the "<b>count ()</b>" Method, has "<b>Appeared</b>" in the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Find" the "Number of Times" an "Element" is "Present" in the "List" Using the "count ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print(myStrList.count("Apple"))

# COMMAND ----------

# MAGIC %md
# MAGIC * If an "<b>Element</b>" is "<b>Searched</b>" that "<b>Does Not Exist</b>" in the "<b>List</b>" using the "<b>count ()</b>" Method, "<b>No Exception</b>" is "<b>Thrown</b>". Instead "<b>0</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Find" the "Number of Times" an "Element" is "Present" in the "List", which "Does Not Exist" in the "List", Using the "count ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print(myStrList.count("Papaya"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Membership Verification" of the "Elements" in a "List"
# MAGIC * "<b>Membership</b>" is a "<b>Fundamental Operation</b>" for "<b>Lists</b>".
# MAGIC * As with "<b>Other Collection Data Types</b>", the "<b>Membership Operation</b>" is "<b>Performed</b>" using the "<b>IN</b>" and "<b>NOT IN</b>" Operators on "<b>Lists</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "List" Using "IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Present</b>" in a "<b>List</b>" using "<b>Membership</b>", then "<b>IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>List</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>List</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Present" in a "List" Using the "IN Operator"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print("Pomengrate" in myStrList)

print("Papaya" in myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "List" Using "NOT IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Not Present</b>" in a "<b>List</b>" using "<b>Membership</b>", then "<b>NOT IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>List</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>List</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Not Present" in a "List" Using the "NOT IN Operator"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
print("Papaya" not in myStrList)

print("Pomengrate" not in myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Remove" the "Elements" from a "List"
# MAGIC * There are "<b>Multiple Methods</b> available to "<b>Remove</b>" the "<b>Elements</b>" from a "<b>List</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "List" Using the "del" Keyword
# MAGIC * It is possible to "<b>Remove</b>" an "<b>Element</b>" from a "<b>List</b>" using the "<b>del Keyword</b>".
# MAGIC * The "<b>del Keyword</b>" "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Index</b>" of the "<b>List Element</b>" to be "<b>Deleted</b>", and, "<b>Removes</b>" the "<b>Element</b>" from the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Third Element", Present at "Index = 2", from the "List" Using the "del Keyword"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
del myStrList[2]
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Index</b>" to be "<b>Deleted</b>" from a "<b>List</b>", using the "<b>del Keyword</b>", is "<b>Not Present</b>" in the "<b>List</b>", the "<b>IndexError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Sixteenth Element", Present at "Index = 15", from the "List" Using the "del Keyword"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
del myStrList[15]
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "List" Using the "remove ()" Method
# MAGIC * It is possible to "<b>Remove</b>" the "<b>Elements</b>" from a "<b>List</b>" "<b>By Value</b>", rather than "<b>By Position</b>", using the "<b>remove ()</b>" Method.
# MAGIC * The "<b>remove ()</b>" Method "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Value</b>" of the "<b>List Element</b>" to be "<b>Deleted</b>", and, "<b>Removes</b>" the "<b>Element</b>" from the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Pomengrate", from the "List" Using the "remove ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myStrList.remove("Pomengrate")
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Value</b>" to be "<b>Deleted</b>" from a "<b>List</b>" is "<b>Present Multiple Times</b>" in the "<b>List</b>", the "<b>remove ()</b>" Method "<b>Removes</b>" the "<b>First Occurrence</b>" of the "<b>List Element</b>" from the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Apple", Which is "Present Multiple Times", from the "List" Using the "remove ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myStrList.remove("Apple")
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Value</b>" to be "<b>Deleted</b>", using the "<b>remove ()</b>" Method, from a "<b>List</b>", is "<b>Not Present</b>" in the "<b>List</b>", the "<b>ValueError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Papaya", Which is "Not Present", in the "List" Using the "remove ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myStrList.remove("Papaya")
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "List" Using the "pop ()" Method
# MAGIC * It is possible to "<b>Remove</b>" an "<b>Element</b>" from a "<b>List</b>" by its "<b>Index</b>" using the "<b>pop ()</b>" Method.
# MAGIC * The "<b>pop ()</b>" Method "<b>Takes</b>" an "<b>Optional Single Parameter</b>", which is the "<b>Index</b>" of the "<b>List Element</b>" to be "<b>Deleted</b>", and, "<b>Returns</b>" the "<b>Deleted Element</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Third Element", Present at "Index = 2", from the "List" Using the "pop ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
deletedFruit = myStrList.pop(2)

print(deletedFruit)
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC * If "<b>No Argument</b>" is "<b>Passed</b>" to the "<b>pop ()</b>" Method, the "<b>Default Index Value</b>", i.e., "<b>-1</b>" is "<b>Passed</b>" as an "<b>Argument</b>" to the "<b>pop ()</b>" Method "<b>Implicitly</b>".
# MAGIC * Hence, if "<b>No Index</b>" is provided, the "<b>pop ()</b>" Method "<b>Removes</b>" and "<b>Returns</b>" the "<b>Last Item</b>" in the "<b>List</b>".
# MAGIC * This "<b>Helps</b>" to "<b>Implement</b>" the "<b>List</b>" as "<b>Stack</b>", i.e., "<b>LIFO</b>" Data Structure - "<b>Last In First Out</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Last Element" from the "List" Using the "pop ()" Method "Without Providing" Any "Index"
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
deletedFruit = myStrList.pop()

print(deletedFruit)
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Index</b>" to be "<b>Deleted</b>" from a "<b>List</b>", using the "<b>pop ()</b>" Method, is "<b>Not Present</b>" in the "<b>List</b>", the "<b>IndexError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Fifteenth Element", Present at "Index = 14", from the "List" Using the "pop ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
deletedFruit = myStrList.pop(14)

print(deletedFruit)
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" "All" the "Elements" from a "List" Using the "clear ()" Method
# MAGIC * It is possible to "<b>Remove</b>" "<b>All</b>" the "<b>Elements</b>" from a "<b>List</b>" using the "<b>clear ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the "Elements" from the "List" Using the "clear ()" Method
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myStrList.clear()
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Concatenate" the "Lists"
# MAGIC * There are "<b>Multiple Techniques</b> available to "<b>Concatenate</b>" the "<b>Lists</b>".
# MAGIC * All of the "<b>Techniques</b>" will work with "<b>Any Iterable Series</b>" on the "<b>Right-Hand Side</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Concatenation" of the "Lists" Using the "Plus Operator"
# MAGIC * It is possible to "<b>Concatenate</b>" the "<b>Lists</b>" using the "<b>Plus Operator</b>", i.e., "<b>+</b>". This "<b>Results</b>" in a "<b>New List</b>", "<b>Without Modification</b>" of the "<b>Lists</b>" that are "<b>Concatenated</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Lists" Using the "Plus Operator", i.e., "+"
myIntList = [25, 10, 5, 20, 15]
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]

newList = myIntList + myStrList
newList

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Concatenation" of the "Lists" Using the "Augmented Assignment Operator"
# MAGIC * It is possible to use the "<b>Augmented Assignment Operator</b>" to "<b>Concatenate</b>" the "<b>Multiple Lists</b>" in "<b>Python</b>".
# MAGIC * In this case, the "<b>Augmented Assignment Operator</b>" will "<b>Modify</b>" the "<b>Left List</b>", i.e., the "<b>Assignee List</b>" "<b>In-Place</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Lists" Using the "Augmented Assignment Operator"
myIntList = [25, 10, 5, 20, 15]
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]

myStrList += myIntList
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Concatenation" of the "Lists" Using the "extend ()" Method
# MAGIC * It is possible to use the "<b>extend ()</b>" Method to "<b>Concatenate</b>" the "<b>Multiple Lists</b>" in "<b>Python</b>".
# MAGIC * In this case, the "<b>extend ()</b>" Method is "<b>Called</b>" on the "<b>Assignee List</b>", and, "<b>Takes</b>" the "<b>Other List</b>" to be "<b>Concatenated</b>" as the "<b>Argument</b>".
# MAGIC * The "<b>extend ()</b>" Method will "<b>Modify</b>" the "<b>Left List</b>", i.e., the "<b>Assignee List</b>" "<b>In-Place</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Lists" Using the "extend ()" Method
myIntList = [25, 10, 5, 20, 15]
myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]

myStrList.extend(myIntList)
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Reverse" the "Elements" of a "List" "In-Place"
# MAGIC * The following "<b>Technique</b> is available to "<b>Reverse</b>" the "<b>Elements</b>" of a "<b>List</b>" "<b>In-Place</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Reverse" the "Elements" of a "List" "In-Place" Using the "reverse ()" Method
# MAGIC * A "<b>List</b>" can simply be "<b>Reversed</b>" "<b>In-Place</b>" by "<b>Calling</b>" the "<b>reverse ()</b>" Method on the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Reverse" the "Elements" of a "List" "In-Place" Using the "reverse ()" Method
myIntList = [25, 10, 5, 20, 15]
myIntList.reverse()
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Sort" the "Elements" of a "List" "In-Place"
# MAGIC * The following "<b>Technique</b> is available to "<b>Sort</b>" the "<b>Elements</b>" of a "<b>List</b>" "<b>In-Place</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Sort" the "Elements" of a "List" "In-Place" Using the "sort ()" Method
# MAGIC * A "<b>List</b>" can simply be "<b>Sorted</b>" in "<b>Ascending Order</b>" "<b>In-Place</b>" by "<b>Calling</b>" the "<b>sort ()</b>" Method on the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "List" in "Ascending Order" "In-Place" Using the "sort ()" Method
myIntList = [25, 10, 5, 20, 15]
myIntList.sort()
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>sort ()</b>" Method "<b>Accepts</b>" "<b>Two Optional Arguments</b>", i.e., "<b>key</b>" and "<b>reverse</b>".
# MAGIC * When "<b>reverse = True</b>" is "<b>Passed</b>" to the "<b>sort ()</b>" Method, the "<b>List</b>" becomes "<b>Sorted</b>" in "<b>Descending Order</b>" "<b>In-Place</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "List" in "Descending Order" "In-Place" Using the "sort ()" Method
myIntList = [25, 10, 5, 20, 15]
myIntList.sort(reverse = True)
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>key</b>" "<b>Optional Argument</b>" actually "<b>Accepts</b>" any "<b>Callable Object</b>", which is then used to "<b>Extract</b>" a "<b>Key</b>" from "<b>Each Item</b>" in the "<b>List</b>".
# MAGIC * The "<b>Items</b>" in the "<b>List</b>" will then be "<b>Sorted</b>" according to the "<b>Relative Ordering</b>" of these "<b>Keys</b>" "<b>In-Place</b>".
# MAGIC * There are several types of "<b>Callable Objects</b>" in "<b>Python</b>". Example - the "<b>len ()</b>" Function is a "<b>Callable Object</b>", whicb is used to "<b>Determine</b>" the "<b>Length</b>" of a "<b>Collection</b>", such as - "<b>String</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "String List" by the "Length of Its Elements" Using the "sort ()" Method
# "Sort" the "List of Strings" by the "Length of Its Elements" by "Passing" the "len" as the "key" Argument

myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myStrList.sort(key = len)
print(myStrList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Reverse" the "Elements" of a "List" into "Copies"
# MAGIC * Sometimes there are some situations where the "<b>Reversal</b>" of "<b>All</b>" the "<b>Elements</b>" in a "<b>List</b>" "<b>In-Place</b>" is "<b>Not Required</b>".
# MAGIC * For the "<b>Out-Of-Place</b>" "<b>Reversal</b>" of "<b>All</b>" the "<b>Elements</b>" in a "<b>List</b>", the "<b>reversed ()</b>" Built-In Python Method is used.
# MAGIC * "<b>Calling</b> the "<b>reversed ()</b>" Built-In Python Method on a "<b>List</b>" "<b>Does Not Return</b>" a "<b>New List</b>". It "<b>Returns</b>" an "<b>Object</b>" of the Type "<b>list_reverseiterator</b>". This "<b>list_reverseiterator</b>" can then be "<b>Passed</b>" to the "<b>list () Constructor</b>" to "<b>Create</b>" an "<b>Actual List</b>".

# COMMAND ----------

# DBTITLE 1,"Reverse" the "Elements" of a "List" "Out-Of-Place" Using the "reversed ()" Built-In Python Method
myIntList = [25, 10, 5, 20, 15]
myNewIntIterable = reversed(myIntList)
myNewIntList = list(myNewIntIterable)

print(myNewIntIterable)
print(myNewIntList)
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Sort" the "Elements" of a "List" into "Copies"
# MAGIC * Sometimes there are some situations where the "<b>Sorting</b>" of "<b>All</b>" the "<b>Elements</b>" of a "<b>List</b>" "<b>In-Place</b>" is "<b>Not Required</b>".
# MAGIC * For the "<b>Out-Of-Place</b>" "<b>Sorting</b>" of "<b>All</b>" the "<b>Elements</b>" of a "<b>List</b>", the "<b>sorted ()</b>" Built-In Python Method is used.
# MAGIC * "<b>Calling</b> the "<b>sorted ()</b>" Built-In Python Method on a "<b>List</b>" "<b>Returns</b>" a "<b>New Sorted List</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "List" in "Ascending Order" "Out-Of-Place" Using the "sorted ()" Built-In Python Method
myIntList = [25, 10, 5, 20, 15]
myNewIntList = sorted(myIntList)

print(myNewIntList)
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>sorted ()</b>" Built-In Python Method "<b>Accepts</b>" "<b>Two Optional Arguments</b>", i.e., "<b>key</b>" and "<b>reverse</b>".
# MAGIC * When "<b>reverse = True</b>" is "<b>Passed</b>" to the "<b>sorted ()</b>" Built-In Python Method, it "<b>Returns</b>" a "<b>New Sorted List</b>" in "<b>Descending Order</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "List" in "Descending Order" "Out-Of-Place" Using the "sorted ()" Built-In Python Method
myIntList = [25, 10, 5, 20, 15]
myNewIntList = sorted(myIntList, reverse = True)

print(myNewIntList)
print(myIntList)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>key</b>" "<b>Optional Argument</b>" actually "<b>Accepts</b>" any "<b>Callable Object</b>", which is then used to "<b>Extract</b>" a "<b>Key</b>" from "<b>Each Item</b>" in the "<b>List</b>".
# MAGIC * The "<b>Items</b>" in the "<b>New Sorted List</b>" will then be "<b>Sorted</b>" according to the "<b>Relative Ordering</b>" of these "<b>Keys</b>".
# MAGIC * There are several types of "<b>Callable Objects</b>" in "<b>Python</b>". Example - the "<b>len ()</b>" Function is a "<b>Callable Object</b>", whicb is used to "<b>Determine</b>" the "<b>Length</b>" of a "<b>Collection</b>", such as - "<b>String</b>".

# COMMAND ----------

# DBTITLE 1,"Sort" the "Elements" of a "String List" by the "Length of Its Elements" Using the "sorted ()" Built-In Python Method
# "Sort" the "List of Strings" by the "Length of Its Elements" by "Passing" the "len" as the "key" Argument

myStrList = ["Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"]
myNewStrList = sorted(myStrList, key = len)

print(myNewStrList)
print(myStrList)
