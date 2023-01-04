# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "List"
# MAGIC * Topic: Introduction to the "List" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "List"?
# MAGIC * "<b>Python Lists</b>" are the "<b>Sequences of Objects</b>".
# MAGIC * Unlike "<b>String</b>", the "<b>Python Lists</b>" are "<b>Mutable</b>". That means, the "<b>Existing Elements</b>" within the "<b>Lists</b>" can be "<b>Replaced</b>" or "<b>Removed</b>", and, "<b>New Elements</b>" can be "<b>Inserted</b>" or "<b>Appended</b>" to the "<b>Lists</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Python Lists" are "Created"?
# MAGIC * "<b>Python Lists</b>" are "<b>Delimited</b>" by "<b>Square Brackets</b>", i.e., "<b>[]</b>".
# MAGIC * The "<b>Items</b>" within the "<b>Lists</b>" are "<b>Separated</b>" by "<b>Commas</b>", i.e., "<b>,</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python List" of "Integers"
[25, 10, 5, 20, 15]

# COMMAND ----------

# DBTITLE 1,Create a "Python List" of "Strings"
['Oindrila', "Soumyajyoti", 'Kasturi', "Rama"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Heterogeneous List"
# MAGIC * "<b>Python Lists</b>" can be "<b>Heterogeneous</b>". That means "<b>Each</b>" of the "<b>Elements</b>" inside a "<b>Python List</b>" can be of "<b>Different Data Types</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Heterogeneous Python List"
[25, 'Oindrila', False, 30.456, 'Kasturi', True, 15]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an "Empty List"
# MAGIC * It is possible to "<b>Create</b>" an "<b>Empty List</b>" by using an "<b>Empty Square Brackets</b>".

# COMMAND ----------

# DBTITLE 1,Create an "Empty Python List"
[]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "List" From "Other Collection Data Types"
# MAGIC * "<b>Python Lists</b>" can be "<b>Created</b>" from "<b>Other Collection Data Types</b>", such as - "<b>Strings</b>", using the "<b>list () Constructor</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python List" From a "String"
list("Oindrila")

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Whitespace Rules" in "Python"
# MAGIC * The significant "<b>Whitespace Rules</b>" in "<b>Python</b>" can at first seem "<b>Very Rigid</b>", but, there is a "<b>Lot of Flexibility</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * If at the "<b>End</b>" of a "<b>List Declaration Line</b>", the "<b>Brackets</b>", "<b>Braces</b>", or, "<b>Parentheses</b>" are "<b>Unclosed</b>", it is possible to "<b>Continue Declaring the List On the Next Line</b>". This can be "<b>Very Useful</b>" for "<b>Long Literal Collections</b>", or, simply to "<b>Improve</b>" the "<b>Readability</b>".

# COMMAND ----------

# DBTITLE 1,"Multi-Line Python List Declaration" Using "Whitespace Rules" in "Python"
['Oindrila',
 'Chakraborty',
 'Bagchi'
]

# COMMAND ----------

# MAGIC %md
# MAGIC * It is allowed to use an "<b>Additional Comma After the Last Element</b>" in a "<b>List Declaration</b>".
# MAGIC * This is an important "<b>Maintainability Feature</b>".

# COMMAND ----------

# DBTITLE 1,Providing "Additional Comma After the Last Element" in a "List Declaration" is "Allowed" in "Python"
['Oindrila',
 'Chakraborty',
 'Bagchi',
]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Retrieve Elements" From a "List"
# MAGIC * It is possible to "<b>Retrieve</b>" the "<b>Elements</b>" of a "<b>List</b>" by using the "<b>Square Brackets</b>", i.e., "<b>[]</b>", with a "<b>Zero-Based Index</b>".
# MAGIC * Example - To "<b>Retrieve</b>" the "<b>First Element</b>" of a "<b>List</b>", "<b>[0]</b>" should be used, to "<b>Retrieve</b>" the "<b>Second Element</b>" of a "<b>List</b>", "<b>[1]</b>" should be used, and so on.

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Third Element" of a "Literal List"
intList = [25, 10, 5, 20, 15]
intList[2]

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Index</b>" of the "<b>Element</b>" to be "<b>Retrieved</b>" from a "<b>List</b>" is "<b>Not Present</b>" in the "<b>List</b>", the "<b>IndexError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Fifteenth Element", Present at "Index = 14", of a "Literal List"
intList = [25, 10, 5, 20, 15]
intList[14]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Negative Indices" in "Python List"
# MAGIC * One "<b>Very Convenient Feature</b>" of "<b>Lists</b>", and, "<b>Other Python Sequences</b>", such as - "<b>Tuples</b>", and, "<b>Strings</b>", is the "<b>Ability</b>" to "<b>Index From the End</b>", as well as the "<b>Ability</b>" to "<b>Index From the Beginning</b>". This is "<b>Achieved</b>" by "<b>Supplying</b>" the "<b>Negative Indices</b>".
# MAGIC * It is possible to "<b>Access</b>" the "<b>Last Element</b>" of a "<b>Python List</b>" using the "<b>Negative Index</b>" as "<b>-1</b>", the "<b>Second Last Element</b>" of a "<b>Python List</b>" using the "<b>Negative Index</b>" as "<b>-2</b>", and, so on.
# MAGIC * The "<b>Negative Indices</b>" approach is "<b>Much More Elegant</b>" than the "<b>Clunky Approach</b>" of "<b>Subtracting 1 From the Length of the Container</b>", which is otherwise used for "<b>Retrieving the Last Element</b>" in "<b>Other Programming Languages</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Third Last Element" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList[-3]

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Indexing</b>" with "<b>-0</b>" is the "<b>Same</b>" as "<b>Indexing</b>" with "<b>0</b>", which "<b>Returns</b>" the "<b>First Element</b>" in a "<b>Python List</b>", because, there is "<b>No Distinction</b>" between "<b>0</b>" and "<b>-0</b>", since the "<b>Negative Indexing</b>" is essentially "<b>1-Based</b>", rather than "<b>0-Based</b>".
# MAGIC * This is good to keep in mind if the "<b>Indices</b>" are calculated with "<b>Moderately Complex Logic</b>", because, "<b>One-Off Errors</b>" can "<b>Creep Into</b>" the "<b>Negative Indexing</b>" fairly easily.

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "First Element" of a "Python List" Using "Negative Indexing" As "-0"
intList = [25, 10, 5, 20, 15]
intList[-0]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Slicing" in "Python List"
# MAGIC * "<b>Slicing</b>" is a "<b>Form</b>" of "<b>Extended Indexing</b>", which allows to "<b>Refer</b>" to a "<b>Portion of a Python List</b>".
# MAGIC * To use "<b>Slicing</b>", the "<b>Start Index</b>", and, the "<b>Stop Index</b>" of a "<b>Half-Open Range</b>", that are "<b>Separated</b>" by a "<b>Colon</b>", i.e., "<b>:</b>", needs to be "<b>Passed</b>" as the "<b>Square Brackets Index Argument</b>".
# MAGIC * The "<b>Stop Index</b>" should always be "<b>1 Beyond the End of the Returned Range</b>" as "<b>Slicing</b>" involves "<b>Half-Open Range</b>".

# COMMAND ----------

# DBTITLE 1,"Slice" the "First Two Elements" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList[1:3]

# The "Stop Index", "3" in this case, is "1 Beyond the End of the Returned Range"

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Slicing</b>" Facility can be "<b>Combined</b>" with the "<b>Negative Indices</b>" as well.
# MAGIC * The "<b>Negative Index</b>" is used as the "<b>Stop Index</b>".
# MAGIC * Example - To "<b>Take All the Elements Except the First and the Last</b>" from a "<b>Python List</b>", the "<b>Slice</b>" should be used as "<b>[1:-1]</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" "All the Elements Except the First and the Last" From a "Python List"
intList = [25, 10, 5, 20, 15]
intList[1:-1]

# COMMAND ----------

# DBTITLE 1,In "Slicing", Providing the "Negative Index" As the "Start Index" Will "Return" an "Empty List"
intList = [25, 10, 5, 20, 15]
intList[-1:1]

# COMMAND ----------

# MAGIC %md
# MAGIC * In "<b>Slicing</b>", both the "<b>Start</b>" and "<b>Stop Indices</b>" are "<b>Optional</b>".
# MAGIC * To "<b>Slice</b>" "<b>All</b>" the "<b>Elements</b>", from a particular "<b>Start Index</b>" to the "<b>End</b>" of the "<b>Python List</b>", "<b>No Number</b>" should be "<b>Put After</b>" the "<b>Colon</b>", i.e., "<b>:</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" "All the Elements" From the "Third Element" Till the "Last Element" From a "Python List"
intList = [25, 10, 5, 20, 15]
intList[2:]

# COMMAND ----------

# MAGIC %md
# MAGIC * To "<b>Slice</b>" "<b>All</b>" the "<b>Elements</b>", from the "<b>Beginning</b>" of a "<b>Python List</b>" "<b>Upto</b>" but "<b>Not Including</b>" a particular "<b>Stop Index</b>", "<b>No Number</b>" should be "<b>Put Before</b>" the "<b>Colon</b>", i.e., "<b>:</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" "All the Elements" From the "Beginning" of a "Python List" "Till But Not Including"  the "Fourth Element"
intList = [25, 10, 5, 20, 15]
intList[:3]

# COMMAND ----------

# MAGIC %md
# MAGIC * Since in "<b>Slicing</b>", both the "<b>Start</b>" and "<b>Stop Indices</b>" are "<b>Optional</b>", it is entirely possible to "<b>Omit</b>" both of the "<b>Start</b>" and "<b>Stop Indices</b>", and, "<b>Retrieve</b>" "<b>All the Elements</b>" of a "<b>Python List</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" "All the Elements" From a "Python List" By "Omitting" both of the "Start" and "Stop Indices"
intList = [25, 10, 5, 20, 15]
intList[:]

# COMMAND ----------

# MAGIC %md
# MAGIC # "Count" the "Number of Elements" of a "Python List"
# MAGIC * It is possible to "<b>Determine</b>" the "<b>Number of Elements</b>" in a "<b>List</b>" using the "<b>Python Built-In Function</b>", i.e., "<b>len ()</b>".

# COMMAND ----------

# DBTITLE 1,"Count" the "Number of Elements" of a "Python List"
intList = [25, 10, 5, 20, 15]
len(intList)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Iterating Over" the "Elements" of "Python List"
# MAGIC * Since "<b>Python Lists</b>" are "<b>Iterables</b>", the "<b>for Loop</b>" can be used to "<b>Iterate Over</b>" the "<b>Elements</b>" of "<b>Python Lists</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Elements" of a "Python List" Using the "for Loop"
intList = [25, 10, 5, 20, 15]

for eachInt in intList:
  print(eachInt)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Add Elements" Into a "Python List"

# COMMAND ----------

# MAGIC %md
# MAGIC ### How "New Elements" Can Be "Added" To The "End" of a "Python List"?
# MAGIC * It is possible to "<b>Add</b>" the "<b>New Element</b>" to the "<b>End</b>" of a "<b>Python List</b>" by using the "<b>append ()</b>" Method.
# MAGIC * The "<b>append ()</b>" Method "<b>Takes</b>" "<b>Only One Argument</b>". Hence, it is possible to "<b>Add</b>" "<b>Only One New Element</b>" to the "<b>End</b>" of a "<b>Python List</b>".

# COMMAND ----------

# DBTITLE 1,"Add" "One New Element" At the "End" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.append(35)
intList

# COMMAND ----------

# DBTITLE 1,"Not Possible" to "Add" "More Than One New Elements" At the "End" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.append(35, 45)
intList

# COMMAND ----------

# MAGIC %md
# MAGIC * However, it is possible to "<b>Add</b>" "<b>More Than One New Elements</b>" to the "<b>End</b>" of a "<b>Python List</b>", if "<b>All</b>" the "<b>New Elements</b>" are "<b>Passed</b>" as a "<b>New Iterable Series</b>" altogether.

# COMMAND ----------

# DBTITLE 1,"Add" "More Than One New Elements As a List" At the "End" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.append([35, 45])
intList

# COMMAND ----------

# DBTITLE 1,"Add" "More Than One New Elements As a Tuple" At the "End" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.append((35, 45))
intList

# COMMAND ----------

# MAGIC %md
# MAGIC ### How "New Elements" Can Be "Inserted" At a "Particular Index" of a "Python List"?
# MAGIC * It is possible to "<b>Insert</b>" the "<b>New Element</b>" at a "<b>Particular Index</b>" of a "<b>Python List</b>" by using the "<b>insert ()</b>" Method.
# MAGIC * The "<b>insert ()</b>" Method "<b>Accepts</b>" the following "<b>Two Arguments</b>" -
# MAGIC <br><b>1</b>. The "<b>Index</b>" of the "<b>New Element</b>" to be "<b>Inserted</b>". 
# MAGIC <br><b>2</b>. The "<b>New Element</b>" itself.

# COMMAND ----------

# DBTITLE 1,"Add" a "New Element" At the "Third Position", i.e., "Index-2" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.insert(2, 35)
intList

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Index</b>", where the "<b>New Element</b>" needs to be "<b>Inserted</b>" into a "<b>List</b>", using the "<b>insert ()</b>" Method, is "<b>Not Present</b>" in the "<b>List</b>", the "<b>New Element</b>" will be "<b>Appended</b>" to the "<b>End</b>" of the "<b>List</b>".

# COMMAND ----------

# DBTITLE 1,"Add" a "New Element" At the "Fifteenth Position", i.e., "Index = 14" of a "Python List"
intList = [25, 10, 5, 20, 15]
intList.insert(14, 35)
print(len(intList))
print(intList)

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Existing Elements" From a "Python List" Can Be "Replaced" With "New Elements"?
# MAGIC * It is possible to "<b>Replace</b>" an "<b>Existing Element</b>" of a "<b>Python List</b>" with a "<b>New Element</b>" by "<b>Assigning</b>" the "<b>New Element</b>" to the "<b>Index Position</b>" of the "<b>Desired Element</b>" to be "<b>Replaced</b>".

# COMMAND ----------

# DBTITLE 1,"Replace" the "Third Element" of a "Python List" With a New Value
intList = [25, 10, 5, 20, 15]
intList[2] = 35
intList

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Existing Elements" From a "Python List" Can Be "Copied" Into "Another Python List"?

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Copying Lists</b>" is "<b>Shallow</b>" by default, i.e., "<b>Copying</b>" only the "<b>References</b>" to the "<b>List Elements</b>", and, "<b>Not</b>" the "<b>List Elements</b>".
# MAGIC * "<b>Assigning</b>" a "<b>New Reference</b>" to the "<b>Reference</b>" of an "<b>Already Existing Python List</b>" "<b>Never Copies</b>" the "<b>Actual List Object</b>", but, merely "<b>Copies</b>" the "<b>Reference</b>" to that "<b>Already Existing Python List Object</b>".

# COMMAND ----------

# DBTITLE 1,"Actual List Object" is "Not Copied". Only the "Reference" is
intList = [25, 10, 5, 20, 15]
newIntList = intList

# The "New List Reference" is "Pointing" to the "Reference" of the "Already Existing Python List". Hence, "Both" of the "List References" are "Equal".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python List" into a "New Python List" Using "Full Slicing"
# MAGIC * The "<b>Full Slicing</b>" of a "<b>List</b>", i.e., "<b>Omitting</b>" both of the "<b>Start</b>" and "<b>Stop Indices</b>", and, "<b>Retrieving</b>" "<b>All the Elements</b>" of a "<b>Python List</b>" needs to be "<b>Performed</b>" to "<b>Copy</b>" that "<b>Existing Python List</b>" to a "<b>New Python List</b>".
# MAGIC * This "<b>Creates</b>" a "<b>New Python List Object</b>" with a "<b>Distinct Identity</b>".
# MAGIC * It is important to understand that although the "<b>Elements</b>" of the "<b>New Python List Object</b>" can be "<b>Independently Modified</b>", the "<b>Elements</b>" within it are the "<b>References</b>" to the "<b>Same Objects</b>", which are "<b>Referred To</b>" by the "<b>Original Python List Object</b>".
# MAGIC <br><img src = '/files/tables/images/list_copy_image.jpg'>

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python List" into a "New Python List" Using "Slicing"
intList = [25, 10, 5, 20, 15]
newIntList = intList[:]

# "Both" of the "New List Reference", and, the "Original List Reference" have the "Equivalent Value".
print(newIntList == intList)

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python List" into a "New Python List" Using "copy ()" Method
# MAGIC * The "<b>copy ()</b>" Method  is "<b>Called On</b>" the "<b>Existing Python List</b>" to "<b>Copy</b>" "<b>All the Elements</b>" of that "<b>Existing Python List</b>" to a "<b>New Python List</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python List" into a "New Python List" Using "copy ()" Method
intList = [25, 10, 5, 20, 15]
newIntList = intList.copy()

# "Both" of the "New List Reference", and, the "Original List Reference" have the "Equivalent Value".
print(newIntList == intList)

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python List" into a "New Python List" Using "list () Constructor"
# MAGIC * "<b>All the Elements</b>" of an "<b>Existing Python List</b>" can be "<b>Copied</b>" into a "<b>New Python List</b>" by "<b>Passing</b>" the "<b>Existing Python List</b>" to the "<b>list () Constructor</b>".
# MAGIC * Although, it is a "<b>Mattre of Taste</b>", still it is "<b>Preferrable</b>" to use the "<b>list () Constructor</b>" approach to "<b>Copy</b>" "<b>All the Elements</b>" of an "<b>Already Existing Python List</b>" into a "<b>New Python List</b>", since, this approach has the "<b>Advantage</b>" of "<b>Working</b>" with "<b>Any Iterable Series</b>" as the "<b>Source</b>", and, "<b>Not Just Lists</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python List" into a "New Python List" Using "list () Constructor"
intList = [25, 10, 5, 20, 15]
newIntList = list(intList)

# "Both" of the "New List Reference", and, the "Original List Reference" have the "Equivalent Value".
print(newIntList == intList)

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shallow Copy
# MAGIC * A "<b>Shallow Copy</b>" is a "<b>Technique</b>" that "<b>Creates</b>" a "<b>New List</b>", which "<b>Contains</b>" the "<b>Same Object References</b>" as the "<b>Source List</b>", but, "<b>Does Not Copy</b>" the "<b>Actual Reffered To Objects</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Lists" with "Immutable Objects"

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Elements</b>" are "<b>Immutable Objects</b>", such as - "<b>String</b>", "<b>Int</b>", "<b>Tuple</b>" etc., then after "<b>Re-Binding</b>", i.e., "<b>Replacing</b>" "<b>Any</b>" of the "<b>Values</b>" that is present at "<b>Any Index</b>" in "<b>Any</b>" of the "<b>List References</b>", the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Changed List Reference</b>" at the "<b>Index Position</b>", where "<b>New Value</b>" is "<b>Replaced</b>", and, the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Unchanged List Reference</b>" at the "<b>Same Index Position</b>", where "<b>Old Value</b>" still "<b>Remains</b>" are "<b>Not Same</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" a "List" using a "Full Slicing" and "Replace" a "Value" in the "Original List"
intList = [25, 10, 5, 20, 15]
newIntList = intList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# "Replace" the "Value" at "Index = 2" in the "Original List Reference".
intList[2] = 35

print(newIntList)
print(intList)

# "Object" that is "Referred To" by the "Original List Reference" at "Index = 2" after "Changing", and, the "Object" that is "Referred To" by the "New List Reference" at "Index = 2" without "Change", are "Not" the "Same" anymore.
print(newIntList[2] is intList[2])

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Elements</b>" are "<b>Immutable Objects</b>", such as - "<b>String</b>", "<b>Int</b>", "<b>Tuple</b>" etc., then after "<b>Mutating</b>", i.e., "<b>Appending</b>" "<b>Any New Value</b>" to the "<b>End</b>" of "<b>Any</b>" of the "<b>List References</b>", or, "<b>Inserting</b>" "<b>Any New Value</b>" at a particular "<b>Index</b>" of "<b>Any</b>" of the "<b>List References</b>", the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Changed List Reference</b>" at that particular "<b>Index Position</b>", where "<b>New Value</b>" is "<b>Appended</b>", or, "<b>Inserted</b>", and, the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Unchanged List Reference</b>" at the "<b>Same Index Position</b>", where "<b>Old Value</b>" still "<b>Remains</b>" are "<b>Not Same</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" a "List" using a "Full Slicing" and "Append" a "Value" at the "End" of the "Original List"
intList = [25, 10, 5, 20, 15]
newIntList = intList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# "Append" the "Value" at the "End" of the "Original List Reference".
intList.append(35)

print(newIntList)
print(intList)

# COMMAND ----------

# DBTITLE 1,"Copy" a "List" using a "Full Slicing" and "Insert" a "Value" at a Particular "Index" of the "Original List"
intList = [25, 10, 5, 20, 15]
newIntList = intList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newIntList is intList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newIntList[0] is intList[0])
print(newIntList[1] is intList[1])
print(newIntList[2] is intList[2])
print(newIntList[3] is intList[3])
print(newIntList[4] is intList[4])

# "Insert" the "Value" at the "Index = 2" in the "Original List Reference".
intList.insert(2, 35)

print(newIntList)
print(intList)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Lists" with "Mutable Objects"

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Elements</b>" are "<b>Mutable Objects</b>", such as - "<b>List</b>" itself, then after "<b>Re-Binding</b>", i.e., "<b>Replacing</b>" "<b>Any</b>" of the "<b>Inner List</b>" that is present at "<b>Any Index</b>" in "<b>Any</b>" of the "<b>Outer List References</b>", the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Changed Outer List Reference</b>" at the "<b>Index Position</b>", where "<b>New Inner List</b>" is "<b>Replaced</b>", and, the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Unchanged Outer List Reference</b>" at the "<b>Same Index Position</b>", where "<b>Old Inner List</b>" still "<b>Remains</b>" are "<b>Not</b> the <b>Same</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" a "Nested List" using a "Full Slicing" and "Replace" a "Value" in the "Original Nested List"
nestedIntList = [[25, 15], [17, 7], [21, 11], [13, 3], [14, 4]]
newNestedIntList = nestedIntList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newNestedIntList is nestedIntList)

# The "List Objects", i.e., "nestedIntList" and "newNestedIntList" are "Two Distinct List Objects", but, with "Equivalent Values".
print(newNestedIntList == nestedIntList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newNestedIntList[0] is nestedIntList[0])
print(newNestedIntList[1] is nestedIntList[1])
print(newNestedIntList[2] is nestedIntList[2])
print(newNestedIntList[3] is nestedIntList[3])
print(newNestedIntList[4] is nestedIntList[4])

# "Replace" the "Inner List Value" at "Index = 2" in the "Original List Reference".
nestedIntList[2] = [35, 25]

print(newNestedIntList)
print(nestedIntList)

# "Object" that is "Referred To" by the "Original List Reference" at "Index = 2" after "Changing", and, the "Object" that is "Referred To" by the "New List Reference" at "Index = 2" without "Change", are "Not" the "Same" anymore.
print(newNestedIntList[2] is nestedIntList[2])

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>List Elements</b>" are "<b>Mutable Objects</b>", such as - "<b>List</b>" itself, then after "<b>Mutating</b>", i.e., "<b>Appending</b>" "<b>Any New Inner List</b>" to the "<b>End</b>" of "<b>Any</b>" of the "<b>Outer List References</b>", or, "<b>Inserting</b>" "<b>Any New Inner List</b>" at a particular "<b>Index</b>" of "<b>Any</b>" of the "<b>Outer List References</b>", the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Changed Outer List Reference</b>" at that particular "<b>Index Position</b>", where "<b>New Inner List</b>" is "<b>Appended</b>", or, "<b>Inserted</b>", and, the "<b>Object</b>" that is "<b>Referred To</b>" by the "<b>Unchanged Outer List Reference</b>" at the "<b>Same Index Position</b>", where "<b>Old Inner List</b>" still "<b>Remains</b>" are "<b>Not</b> the <b>Same</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" a "Nested List" using a "Full Slicing" and "Append" a "Value" at the "End" of "One of the Original Nested List"
nestedIntList = [[25, 15], [17, 7], [21, 11], [13, 3], [14, 4]]
newNestedIntList = nestedIntList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newNestedIntList is nestedIntList)

# The "List Objects", i.e., "nestedIntList" and "newNestedIntList" are "Two Distinct List Objects", but, with "Equivalent Values".
print(newNestedIntList == nestedIntList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newNestedIntList[0] is nestedIntList[0])
print(newNestedIntList[1] is nestedIntList[1])
print(newNestedIntList[2] is nestedIntList[2])
print(newNestedIntList[3] is nestedIntList[3])
print(newNestedIntList[4] is nestedIntList[4])

# "Append" the Value "1" at the "End" of the "Inner List", Present at the "Index = 2" in the "Original List Reference".
nestedIntList[2].append(1)

print(nestedIntList)
print(newNestedIntList)

# "Object" that is "Referred To" by the "Changed Outer List Reference" at "Index = 2" after "Appending", and, the "Object" that is "Referred To" by the "Unchanged Outer List Reference" at "Index = 2" without "Appending", are the "Same".
print(newNestedIntList[2] is nestedIntList[2])

# COMMAND ----------

# DBTITLE 1,"Copy" a "List" using a "Full Slicing" and "Insert" a "Value" at a Particular "Index" in "One of the Original Nested List"
nestedIntList = [[25, 15], [17, 7], [21, 11], [13, 3], [14, 4]]
newNestedIntList = nestedIntList[:]

# The "New List Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python List".
print(newNestedIntList is nestedIntList)

# The "List Objects", i.e., "nestedIntList" and "newNestedIntList" are "Two Distinct List Objects", but, with "Equivalent Values".
print(newNestedIntList == nestedIntList)

# "Objects" that are "Referred To" by "Both" of the "New List Reference" and the "Original List Reference", Present at "Each Index", are the "Same".
print(newNestedIntList[0] is nestedIntList[0])
print(newNestedIntList[1] is nestedIntList[1])
print(newNestedIntList[2] is nestedIntList[2])
print(newNestedIntList[3] is nestedIntList[3])
print(newNestedIntList[4] is nestedIntList[4])

# "Insert" the Value "1" at the "Second Position", i.e., "Index = 1" of the "Inner List", Present at the "Index = 2" in the "Original Outer List Reference".
nestedIntList[2].insert(1, 1)

print(nestedIntList)
print(newNestedIntList)

# "Object" that is "Referred To" by the "Changed Outer List Reference" at "Index = 2" after "Appending", and, the "Object" that is "Referred To" by the "Unchanged Outer List Reference" at "Index = 2" without "Appending", are the "Same".
print(newNestedIntList[2] is nestedIntList[2])
