# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Dictionary"
# MAGIC * Topic: Introduction to the "Dictionary" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Dictionary"?
# MAGIC * A "<b>Python Dictionary</b>" "<b>Maps</b>" the "<b>Keys</b>" to the "<b>Values</b>", and, in "<b>Other Programming Languages</b>", knows as a "<b>Map</b>", or, an "<b>Associative Array</b>".
# MAGIC * Each of the "<b>Key-Value Pair</b>" in a "<b>Python Dictionary</b>" is "<b>Called</b>" an "<b>Item</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Python Dictionaries" are "Created"?
# MAGIC * "<b>Python Dictionaries</b>" are "<b>Created</b>" using the "<b>Curly Braces</b>", i.e., "<b>{}</b>", "<b>Containing</b>" the "<b>Key-Value Pairs</b>".
# MAGIC * "<b>Each</b>" of the "<b>Pairs</b>" within a "<b>Dictionary</b>" is "<b>Separated</b>" by a "<b>Comma</b>", i.e., "<b>,</b>".
# MAGIC * "<b>Each</b>" of the "<b>Keys</b>" within "<b>Each</b>" of the "<b>Pairs</b>" in a "<b>Dictionary</b>" is "<b>Separated</b>" from the corresponding "<b>Values</b>" by a "<b>Colon</b>", i.e., "<b>:</b>".
# MAGIC * The "<b>Values</b>" in "<b>Each</b>" of the "<b>Key-Value Pairs</b>" are "<b>Accessible</b>" via the corresponding "<b>Keys</b>".
# MAGIC * Since, "<b>Each Key</b>" is "<b>Associated</b>" with "<b>Exactly One Value</b>", and, "<b>Lookup</b>" in a "<b>Python Dictionary</b>" is "<b>Performed</b>" through the "<b>Keys</b>", the "<b>Keys</b>" must be "<b>Unique</b>" within any "<b>Python Dictionary</b>".
# MAGIC * It is fine to have "<b>Duplicate Values</b>" in a "<b>Python Dictionary</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Dictionary" of "Persons", With "Name" as "Key" and "Age" as "Value"
{
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an "Empty Dictionary"
# MAGIC * It is possible to "<b>Create</b>" an "<b>Empty Dictionary</b>" by using an "<b>Empty Curly Braces</b>".

# COMMAND ----------

# DBTITLE 1,Create an "Empty Python Dictionary"
{}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Dictionary" From "Other Collection Data Types"
# MAGIC * "<b>Python Dictionaries</b>" can be "<b>Created</b>" from "<b>Other Collection Data Types</b>", such as - "<b>Tuples</b>", using the "<b>dict () Constructor</b>".
# MAGIC * The "<b>dict () Constructor</b>" can be used to "<b>Convert</b>" from an "<b>Iterable Series</b>" of "<b>Key-Value Pairs</b>" that are "<b>Stored</b>" in "<b>Other Collection Data Types</b>", such as - "<b>Tuples</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Dictionary" From an "Iterable Series", i.e., "List" of  "Tuple"
dict([("Oindrila", 34), ("Soumyajyoti", 35), ("Premanshu", 66), ("Rama", 61), ("Kasturi", 29)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Dictionary" From "Keyword Arguments"
# MAGIC * So long as the "<b>Keys</b>" in a "<b>Python Dictionary</b>" are "<b>Legitimate Python Identifiers</b>", it is even possible to "<b>Create</b>" a "<b>Python Dictionary</b>" directly from "<b>Keyword Arguments</b>" that are "<b>Passed</b>" to the "<b>dict () Constructor</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Dictionary" From "Keyword Arguments"
dict(Oindrila = 34, Soumyajyoti = 35, Premanshu = 66, Rama = 61, Kasturi = 29)

# COMMAND ----------

# MAGIC %md
# MAGIC # Python Dictionary Internals
# MAGIC * Internally, the "<b>Python Dictionary</b>" "<b>Maintains</b>" the "<b>Pairs</b>" of "<b>References</b>" to the "<b>Key</b>" Objects, and, the "<b>Value</b>" Objects respectively.
# MAGIC * The "<b>Key</b>" Objects must be "<b>Immutable</b>". So, "<b>Strings</b>", "<b>Numbers</b>", and, "<b>Tuples</b>" can be used as "<b>Key</b>", but, "<b>Lists</b>" can not be used.
# MAGIC * The "<b>Value</b>" Objects can be "<b>Mutable</b>", and, in practice, often are.

# COMMAND ----------

# MAGIC %md
# MAGIC # "Order" of "Items" in "Python Dictionary"
# MAGIC * In "<b>Python Versions Prior to 3.7</b>", the "<b>Order</b>" of the "<b>Items</b>" in a "<b>Python Dictionary</b>" "<b>Couldn't</b>" be relied upon. It is essentially "<b>Random</b>", and, many even "<b>Vary</b>" between the "<b>Different Runs</b>" of the "<b>Same Program</b>".
# MAGIC * As of "<b>Python Versions 3.7</b>", however, the "<b>Order</b>" of the "<b>Items</b>" in a "<b>Python Dictionary</b>" are required to be "<b>Kept</b>" in the "<b>Insertion Order</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Retrieve" the "Values" of "Items" By Corresponding "Keys" From a "Dictionary"
# MAGIC * It is possible to "<b>Retrieve</b>" the "<b>Value</b>" of an "<b>Item</b>" by the corresponding "<b>Key</b>" from a "<b>Dictionary</b>" using the "<b>Square Brackets Operator</b>", i.e., "<b>[]</b>".
# MAGIC * If the "<b>Data Type</b>" of the "<b>Key</b>" is "<b>String</b>", then the "<b>Key</b>" should be "<b>Put</b>" inside the "<b>Square Brackets Operator</b>" "<b>Inside Quotation</b>".
# MAGIC * Example - To "<b>Retrieve</b>" the "<b>Value</b>" of an "<b>Item</b>" by the corresponding "<b>Key</b>", e.g., "<b>Soumyajyoti</b>", from a "<b>Dictionary</b>", the "<b>Key</b>", i.e., "<b>Soumyajyoti</b>" should be "<b>Put</b>" inside the "<b>Square Brackets Operator</b>" as following - "<b>["Soumyajyoti"]</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key" in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

personDict['Soumyajyoti']

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" to be "<b>Retrieved</b>" from a "<b>Dictionary</b>" is "<b>Not Present</b>" in the "<b>Dictionary</b>", the "<b>KeyError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key", Which is "Not Present", in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

personDict['Avishek']

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Retrieve" the "Values" of "Items" By Corresponding "Keys" From a "Dictionary" Using "get ()" Method
# MAGIC * It is possible to "<b>Retrieve</b>" the "<b>Value</b>" of an "<b>Item</b>" by the corresponding "<b>Key</b>" from a "<b>Dictionary</b>" using the "<b>get ()</b>" Method.
# MAGIC * If the "<b>Data Type</b>" of the "<b>Key</b>" is "<b>String</b>", then the "<b>Key</b>" should be "<b>Put</b>" inside the "<b>get ()</b>" Method "<b>Inside Quotation</b>".
# MAGIC * If the "<b>Data Type</b>" of the "<b>Key</b>" is "<b>Integer</b>", then the "<b>Key</b>" should be "<b>Put</b>" inside the "<b>get ()</b>" Method "<b>Without Quotation</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key" of Type "String" in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

personDict.get('Soumyajyoti')

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key" of Type "Integer" in a "Literal Dictionary"
personDict = {
  34: "Oindrila",
  35: "Soumyajyoti",
  66: "Premanshu",
  61: "Rama",
  29: "Kasturi"
}

personDict.get(35)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" to be "<b>Retrieved</b>" from a "<b>Dictionary</b>", using the "<b>get ()</b>" Method, is "<b>Not Present</b>" in the "<b>Dictionary</b>", "<b>No Exception</b>" is "<b>Thrown</b>". Instead "<b>None</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key", Which is "Not Present", in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(personDict.get('Avishek'))

# COMMAND ----------

# MAGIC %md
# MAGIC * When "<b>Retrieving</b>" the "<b>Value</b>" of an "<b>Item</b>" by "<b>Key</b>" from a "<b>Dictionary</b>", using the "<b>get ()</b>" Method, if the "<b>Key</b>" of the "<b>Item</b>" to be "<b>Retrieved</b>" is "<b>Not Present</b>" in the "<b>Dictionary</b>", it is possible to assign a "<b>Default Value</b>" to be "<b>Returned</b>" instead "<b>None</b>".
# MAGIC * Provide the "<b>Default Value</b>" to be "<b>Returned</b>" as the "<b>Second Argument</b>" to the "<b>get ()</b>" Method.
# MAGIC * When the "<b>Default Value</b>" is provided as the "<b>Second Argument</b>", if the "<b>Key</b>" of an "<b>Item</b>" to be "<b>Retrieved</b>" is "<b>Not Present</b>" in the "<b>Dictionary</b>", then the "<b>Default Value</b>" will be "<b>Returned</b>".
# MAGIC * When the "<b>Default Value</b>" is provided as the "<b>Second Argument</b>", if the "<b>Key</b>" of an "<b>Item</b>" to be "<b>Retrieved</b>" is "<b>Present</b>" in the "<b>Dictionary</b>", then the corresponding "<b>Value</b>" will be "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key", Which is "Present", in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(personDict.get('Soumyajyoti', 36))

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Value" of the "Item" by the "Key", Which is "Not Present", in a "Literal Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(personDict.get('Avishek', 36))

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Existing Values" of "Items" Can Be "Replaced" With "New Values" By Corresponding "Keys" From a "Dictionary"?
# MAGIC * It is possible to "<b>Replace</b>" the "<b>Existing Value</b>" of an "<b>Item</b>" with a "<b>New Value</b>" by "<b>Putting</b>" the associated "<b>Key</b>" inside the "<b>Square Brackets Operator</b>" and "<b>Assigning</b>" the "<b>New Value</b>" to it.

# COMMAND ----------

# DBTITLE 1,"Replace" the "Value" of the "Item" With the Corresponding "Key", i.e., "Rama" to "60" in a "Python Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(personDict)

personDict['Rama'] = 60
print(personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Add New Items" Into a "Python Dictionary"
# MAGIC * If a "<b>New Value</b>" is "<b>Assigned</b>" to a "<b>Key</b>", which "<b>Does Not Exist</b>" in the "<b>Python Dictionary</b>", by "<b>Putting</b>" the "<b>Key</b>" inside the "<b>Square Brackets Operator</b>" and "<b>Assigning</b>" the "<b>New Value</b>" to it, then a "<b>New Item</b>" would be "<b>Added</b>" into the "<b>Python Dictionary</b>".

# COMMAND ----------

# DBTITLE 1,"Add" a "New Item" into a "Python Dictionary"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print(personDict)

personDict['Dola'] = 61
print(personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Add" the "Items" From "One Dictionary" to "Another Dictionary"
# MAGIC * It is possible to "<b>Extend</b>" an "<b>Existing Python Dictionary</b>" with the "<b>Definitions</b>" from "<b>Another Existing Python Dictionary</b>" using the "<b>update ()</b>" Method.
# MAGIC * The "<b>update ()</b>" Method is "<b>Called On</b>" the "<b>Dictionary</b>" "<b>To Be Updated</b>", and, "<b>Takes</b>" the "<b>Contents</b>" of the "<b>Dictionary</b>", which needs to be "<b>Merged In</b>", as the "<b>Argument</b>".

# COMMAND ----------

# DBTITLE 1,"Add" the "Items" From "One Dictionary" to "Another Dictionary" Using "update ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

newPersonDict = {
  "Avishek": 35,
  "Avirupa": 36,
  "Rahul": 35,
  "Paulomi": 33,
  "Ameer": 36,
  "Megha": 33
}

personDict.update(newPersonDict)

print(personDict)
print(newPersonDict)

# COMMAND ----------

# MAGIC %md
# MAGIC * In the "<b>Argument</b>" of the "<b>update ()</b>" Method, if the "<b>Contents</b>" of the "<b>Dictionary</b>" that needs to be "<b>Merged In</b>" with the "<b>Target Dictionary</b>" includes the "<b>Keys</b>", which are "<b>Already Present</b>" in the "<b>Target Dictionary</b>", then the "<b>Values</b>" associated with these "<b>Keys</b>" are "<b>Replaced</b>" in the "<b>Target Dictionary</b>" by the corresponding "<b>Values</b>" from the "<b>Source Dictionary</b>" that is "<b>Sent</b>" as the "<b>Argument</b>" to the "<b>update ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Add" the "Items" From "One Dictionary" to "Another Dictionary", Having "Duplicate Keys", Using "update ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

newPersonDict = {
  "Avishek": 35,
  "Avirupa": 36,
  "Rahul": 35,
  "Oindrila": 33,
  "Soumyajyoti": 36,
  "Megha": 33
}

personDict.update(newPersonDict)

print(personDict)
print(newPersonDict)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Iterating Over" the "Items" of "Python Dictionary"
# MAGIC * Since "<b>Python Dictionaries</b>" are "<b>Iterables</b>", the "<b>for Loop</b>" can be used to "<b>Iterate Over</b>" the "<b>Items</b>" of "<b>Python Dictionaries</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Iterating Over" the "Items" of "Python Dictionary" Using "Keys" on "Each Iteration"
# MAGIC * The "<b>Python Dictionary</b>" "<b>Yields</b>" the "<b>Key</b>" on "<b>Each Iteration</b>", and, the corresponding "<b>Value</b>" can be "<b>Retrieved</b>" through "<b>Look up</b>" using the "<b>Square Brackets Operator</b>".
# MAGIC * It is possible to "<b>Iterate Over</b>" the "<b>Keys</b>" of a "<b>Python Dictionary</b>" using the "<b>for Loop</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Keys" of a "Python Dictionary" Using the "for Loop"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

for personKey in personDict:
  print(f"Age of {personKey} is {personDict[personKey]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Iterating Over" the "Items" of "Python Dictionary" to "Retrieve" Only "Keys" Using "keys ()" Method
# MAGIC * It is possible to "<b>Iterate Over</b>" only the "<b>Keys</b>" of a "<b>Python Dictionary</b>" by "<b>Calling</b>" the "<b>keys ()</b>" Method on the "<b>Python Dictionary</b>" in the "<b>for Loop</b>".
# MAGIC * The "<b>keys ()</b>" Method "<b>Returns</b>" an "<b>Object</b>", which provides an "<b>Iterable View</b>" onto the "<b>Keys</b>" of "<b>Python Dictionary</b>", "<b>Without</b>" causing the "<b>Keys</b>" to be "<b>Copied</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Keys" of a "Python Dictionary" Using "keys ()" Method in the "for Loop"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

for personKey in personDict.keys():
  print(f"Name of the Person is {personKey}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Iterating Over" the "Items" of "Python Dictionary" to "Retrieve" Only "Values" Using "values ()" Method
# MAGIC * It is possible to "<b>Iterate Over</b>" only the "<b>Values</b>" of a "<b>Python Dictionary</b>" by "<b>Calling</b>" the "<b>values ()</b>" Method on the "<b>Python Dictionary</b>" in the "<b>for Loop</b>".
# MAGIC * The "<b>values ()</b>" Method "<b>Returns</b>" an "<b>Object</b>", which provides an "<b>Iterable View</b>" onto the "<b>Values</b>" of "<b>Python Dictionary</b>", "<b>Without</b>" causing the "<b>Values</b>" to be "<b>Copied</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Values" of a "Python Dictionary" Using "values ()" Method in the "for Loop"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

for personAge in personDict.values():
  print(f"Age is {personAge}")

# COMMAND ----------

# MAGIC %md
# MAGIC * There is "<b>No Efficient / Convenient Way</b>" to "<b>Retrieve</b>" the corresponding "<b>Key</b>" from a "<b>Value</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Iterating Over" the "Keys" and "Values" of "Python Dictionary" in "Tandem" Using "items ()" Method
# MAGIC * It is possible to "<b>Iterate Over</b>" the "<b>Keys</b>", and, the "<b>Values</b>" of a "<b>Python Dictionary</b>" in "<b>Tandem</b>".
# MAGIC * It is possible to get hold of an "<b>Iterable View</b>" of the "<b>Items</b>" of a "<b>Python Dictionary</b>" using the "<b>items ()</b>" Method.
# MAGIC * When "<b>Iterated</b>", the "<b>Items View</b>" "<b>Yields</b>" "<b>Each</b>" of the "<b>Key-Value Pairs</b>" of a "<b>Python Dictionary</b>" as a "<b>Tuple</b>".
# MAGIC * By using the "<b>Tuple Unpacking</b>" in the "<b>for Statement</b>", it is possible to "<b>Retrieve</b>" both the "<b>Keys</b>", and, the "<b>Values</b>" of a "<b>Python Dictionary</b>" in "<b>One Operation</b>" "<b>Without</b>" the "<b>Extra Look up</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Keys", and, "Values" of a "Python Dictionary" Using "items ()" Method in the "for Loop"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

for personKey, personAge in personDict.items():
  print(f"Age of {personKey} is {personAge}")

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Existing Items" From a "Python Dictionary" Can Be "Copied" Into "Another Python Dictionary"?

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Copying Dictionaries</b>" is "<b>Shallow</b>" by default, i.e., "<b>Copying</b>" only the "<b>References</b>" to the "<b>Key</b>" and "<b>Value</b>" Objects in the "<b>Items</b>", and, "<b>Not</b>" the "<b>Items</b>".
# MAGIC * There are "<b>Two Ways</b>" of "<b>Copying Dictionaries</b>", of which, the "<b>Second Way</b>" is the "<b>Most Common</b>" one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python Dictionary" into a "New Python Dictionary" Using "copy ()" Method
# MAGIC * The "<b>copy ()</b>" Method  is "<b>Called On</b>" the "<b>Existing Python Dictionary</b>" to "<b>Copy</b>" "<b>All the Items</b>" of that "<b>Existing Python Dictionary</b>" to a "<b>New Python Dictionary</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python Dictionary" into a "New Python Dictionary" Using "copy ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

newPersonDict = personDict.copy()

print(personDict)
print(newPersonDict)

# "Both" of the "New Dictionary Reference", and, the "Original Dictionary Reference" have the "Equivalent Value".
print(newPersonDict == personDict)

# The "New Dictionary Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python Dictionary".
print(newPersonDict is personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python Dictionary" into a "New Python Dictionary" Using "dict () Constructor"
# MAGIC * "<b>All the Items</b>" of an "<b>Existing Python Dictionary</b>" can be "<b>Copied</b>" into a "<b>New Python Dictionary</b>" by "<b>Passing</b>" the "<b>Existing Python Dictionary</b>" to the "<b>dict () Constructor</b>".
# MAGIC * Although, it is a "<b>Mattre of Taste</b>", still it is "<b>Preferrable</b>" to use the "<b>dict () Constructor</b>" approach to "<b>Copy</b>" "<b>All the Items</b>" of an "<b>Already Existing Python Dictionary</b>" into a "<b>New Python Dictionary</b>", since, this approach has the "<b>Advantage</b>" of "<b>Working</b>" with "<b>Any Iterable Series</b>" as the "<b>Source</b>", and, "<b>Not Just Dictionaries</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python Dictionary" into a "New Python Dictionary" Using "dict () Constructor"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

newPersonDict = dict(personDict)

print(personDict)
print(newPersonDict)

# "Both" of the "New Dictionary Reference", and, the "Original Dictionary Reference" have the "Equivalent Value".
print(newPersonDict == personDict)

# The "New Dictionary Reference" is "Not Pointing" to the "Reference" of the "Already Existing Python Dictionary".
print(newPersonDict is personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Membership Verification" of the "Items" in a "Dictionary"
# MAGIC * The "<b>Membership Tests</b>" for "<b>Python Dictionaries</b>" work on the "<b>Keys</b>".
# MAGIC * "<b>Membership</b>" is a "<b>Fundamental Operation</b>" for "<b>Dictionaries</b>".
# MAGIC * As with "<b>Other Collection Data Types</b>", the "<b>Membership Operation</b>" is "<b>Performed</b>" using the "<b>IN</b>" and "<b>NOT IN</b>" Operators on "<b>Dictionaries</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Keys" of an "Item" in a "Dictionary" Using "IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" a "<b>Key</b>" of an "<b>Item</b>" is "<b>Present</b>" in a "<b>Dictionary</b>" using "<b>Membership</b>", then "<b>IN Operator</b>" is used.
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" is "<b>Present</b>" in the "<b>Dictionary</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" is "<b>Not Present</b>" in the "<b>Dictionary</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If  a "Key" of an "Item" is "Present" in a "Dictionary" Using the "IN Operator"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print("Soumyajyoti" in personDict)

print("Avishek" in personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Keys" of an "Item" in a "Dictionary" Using "NOT IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" a "<b>Key</b>" of an "<b>Item</b>" is "<b>Not Present</b>" in a "<b>Dictionary</b>" using "<b>Membership</b>", then "<b>NOT IN Operator</b>" is used.
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" is "<b>Not Present</b>" in the "<b>Dictionary</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Key</b>" of an "<b>Item</b>" is "<b>Present</b>" in the "<b>Dictionary</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If  a "Key" of an "Item" is "Not Present" in a "Dictionary" Using the "NOT IN Operator"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

print("Avishek" not in personDict)

print("Soumyajyoti" not in personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Remove" the "Items" from a "Dictionary"
# MAGIC * There are "<b>Multiple Methods</b> available to "<b>Remove</b>" the "<b>Items</b>" from a "<b>Dictionary</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Items" from a "Dictionary" Using the "del" Keyword
# MAGIC * It is possible to "<b>Remove</b>" an "<b>Items</b>" from a "<b>Dictionary</b>" using the "<b>del Keyword</b>".
# MAGIC * The "<b>del Keyword</b>" "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Key</b>" of the "<b>Item</b>" to be "<b>Deleted</b>" from the "<b>Dictionary</b>", and, "<b>Removes</b>" the "<b>Item</b>" from the "<b>Dictionary</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Item", Having "Oindrila" as "Key", from the "Dictionary" Using the "del Keyword"
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

del personDict["Oindrila"]

print(personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Items" from a "Dictionary" Using the "pop ()" Method
# MAGIC * It is possible to "<b>Remove</b>" an "<b>Items</b>" from a "<b>Dictionary</b>" by its "<b>Key</b>" using the "<b>pop ()</b>" Method.
# MAGIC * The "<b>pop ()</b>" Method "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Key</b>" of the "<b>Item</b>" to be "<b>Deleted</b>", and, "<b>Returns</b>" the "<b>Value</b>" of the "<b>Deleted Item</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Item", Having "Oindrila" as "Key", from the "Dictionary" Using the "pop ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

deletedPersonAge = personDict.pop("Oindrila")

print(deletedPersonAge)
print(personDict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Last Item" from a "Dictionary" Using the "popitem ()" Method
# MAGIC * It is possible to "<b>Remove</b>" the "<b>Last Item</b>" from a "<b>Dictionary</b>" using the "<b>popitem ()</b>" Method.
# MAGIC * The "<b>popitem ()</b>" Method "<b>Does Not Take</b>" a "<b>Parameter</b>", and, "<b>Returns</b>" the "<b>Deleted Item</b>" as a "<b>Tuple</b>".
# MAGIC * Hence, by using the "<b>Tuple Unpacking</b>", it is possible to "<b>Retrieve</b>" both the "<b>Key</b>", and, the "<b>Value</b>" of the "<b>Deleted Item</b>" from the "<b>Python Dictionary</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Last Item" from the "Dictionary" Using the "popitem ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

deletedPersonAge = personDict.popitem()

print(deletedPersonAge)
print(personDict)

deletedName, deletedAge = personDict.popitem()
print(deletedName)
print(deletedAge)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" "All" the "Items" from a "Dictionary" Using the "clear ()" Method
# MAGIC * It is possible to "<b>Remove</b>" "<b>All</b>" the "<b>Items</b>" from a "<b>Dictionary</b>" using the "<b>clear ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the "Items" from the "Dictionary" Using the "clear ()" Method
personDict = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

personDict.clear()

print(personDict)
