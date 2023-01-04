# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Set"
# MAGIC * Topic: Introduction to the "Set" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Set"?
# MAGIC * A "<b>Python Set</b>" Data Type is an "<b>Un-Ordered Collection</b>" of "<b>Unique Elements</b>".
# MAGIC * The "<b>Collection</b>" in a "<b>Python Set</b>" are "<b>Mutable</b>", i.e., "<b>Elements</b>" can be "<b>Added</b>", or, "<b>Removed</b>" from a "<b>Python Set</b>".
# MAGIC * "<b>Each</b>" of the "<b>Elements</b>" in a "<b>Python Set</b>" must be "<b>Immutable</b>", like the "<b>Keys</b>" of a "<b>Python Dictionary</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "Python Sets" are "Used"?
# MAGIC * A common use of "<b>Python Set</b>" is to "<b>Efficiently Remove</b>" the "<b>Duplicate Elements</b>" from a "<b>Series</b>" of "<b>Objects</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Python Sets" are "Created"?
# MAGIC * "<b>Python Sets</b>" have a "<b>Literal Form</b>" that is "<b>Very Similar</b>" to the "<b>Python Dictionaries</b>".
# MAGIC * "<b>Python Sets</b>" are "<b>Delimited</b>" by "<b>Curly Braces</b>", i.e., "<b>{}</b>".
# MAGIC * "<b>Each</b>" of the "<b>Items</b>" within a "<b>Set</b>" is a "<b>Single Object</b>", rather than a "<b>Pair Joined by a Colon</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Set" of "Integers"
{25, 10, 5, 20, 15}

# COMMAND ----------

# DBTITLE 1,Create a "Python Set" of "Strings"
{'Oindrila', "Soumyajyoti", 'Kasturi', "Rama"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Heterogeneous Set"
# MAGIC * "<b>Python Sets</b>" can be "<b>Heterogeneous</b>". That means "<b>Each</b>" of the "<b>Elements</b>" inside a "<b>Python Set</b>" can be of "<b>Different Data Types</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Heterogeneous Python Set"
{25, 'Oindrila', False, 30.456, 'Kasturi', True, 15}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an "Empty Set"
# MAGIC * It is "<b>Not Possible</b>" to "<b>Create</b>" an "<b>Empty Set</b>" by using an "<b>Empty Curly Braces</b>", because, an "<b>Empty Curly Braces</b>" "<b>Creates</b>" an "<b>Empty Dictionary</b>".
# MAGIC * In order to "<b>Create</b>" an "<b>Empty Set</b>", the "<b>set () Constructor</b>" is used, where the "<b>set () Constructor</b>" "<b>Does Not Take</b>" any "<b>Argument</b>".
# MAGIC * "<b>set ()</b>" is the "<b>Form</b>" that "<b>Python</b>" "<b>Echoes Back</b>" as an "<b>Empty Set</b>".

# COMMAND ----------

# DBTITLE 1,Create an "Empty Python Set"
set()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a "Set" From "Other Collection Data Types"
# MAGIC * "<b>Python Sets</b>" can be "<b>Created</b>" from "<b>Other Collection Data Types</b>", such as - "<b>Strings</b>", "<b>Lists</b>", using the "<b>set () Constructor</b>", and, the "<b>Duplicate Elements</b>" are "<b>Discarded</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Python Set" From a "String"
set("Oindrila")

# COMMAND ----------

# DBTITLE 1,Create a "Python Set" From a "List"
set([25, 10, 5, 25, 20, 5, 15])

# COMMAND ----------

# MAGIC %md
# MAGIC # "Count" the "Number of Elements" of a "Python Set"
# MAGIC * It is possible to "<b>Determine</b>" the "<b>Number of Elements</b>" in a "<b>Set</b>" using the "<b>Python Built-In Function</b>", i.e., "<b>len ()</b>".

# COMMAND ----------

# DBTITLE 1,"Count" the "Number of Elements" of a "Python Set"
intSet = {25, 10, 5, 20, 15}
len(intSet)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Iterating Over" the "Elements" of "Python Set"
# MAGIC * Since "<b>Python Sets</b>" are "<b>Iterables</b>", the "<b>for Loop</b>" can be used to "<b>Iterate Over</b>" the "<b>Elements</b>" of "<b>Python Sets</b>".

# COMMAND ----------

# DBTITLE 1,"Iterate Over" the "Elements" of a "Python Set" Using the "for Loop"
intSet = {25, 10, 5, 20, 15}

for eachInt in intSet:
  print(eachInt)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Add Elements" Into a "Python Set"

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Add" a "Single New Element" To a "Python Set" Using "add ()" Method
# MAGIC * It is possible to "<b>Add</b>" a "<b>Single New Element</b>" to a "<b>Python Set</b>" by using the "<b>add ()</b>" Method.
# MAGIC * The "<b>add ()</b>" Method "<b>Takes</b>" "<b>Only One Argument</b>", i.e., the "<b>Element</b>" to be "<b>Added</b>" to the "<b>Python Set</b>".

# COMMAND ----------

# DBTITLE 1,"Add" "One New Element" to a "Python Set"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.add(35)
intSet

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Adding</b>" an "<b>Element</b>" using the "<b>add ()</b>" Method, that "<b>Already Exists</b>" in the "<b>Python Set</b>" has "<b>No Effect</b>", and, "<b>Neither</b>" does it "<b>Produces</b>" an "<b>Error</b>".

# COMMAND ----------

# DBTITLE 1,"Add" "One New Element" to a "Python Set", That "Already Exists"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.add(25)
intSet

# COMMAND ----------

# DBTITLE 1,"Not Possible" to "Add" "More Than One New Elements" to a "Python Set"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.add(35, 45)
intSet

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Add" "Multiple New Elements" To a "Python Set" Using "update ()" Method
# MAGIC * It is possible to "<b>Add</b>" "<b>Multiple New Elements</b>" to a "<b>Python Set</b>" using the "<b>update ()</b>" Method.
# MAGIC * The "<b>update ()</b>" Method "<b>Takes</b>" "<b>Only One Argument</b>", i.e., an "<b>Iterable Series</b>" that "<b>Holds</b>" the "<b>Multiple New Elements</b>" to be "<b>Added</b>" to the "<b>Python Set</b>".

# COMMAND ----------

# DBTITLE 1,"Add" "More Than One New Elements As a List" to a "Python Set"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.update([35, 45])
intSet

# COMMAND ----------

# DBTITLE 1,"Add" "More Than One New Elements As a Tuple" to a "Python Set"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.update((35, 45))
intSet

# COMMAND ----------

# DBTITLE 1,"Add" "More Than One New Elements As a Set" to a "Python Set"
intSet = {25, 10, 5, 25, 20, 15, 5}
intSet.update({35, 45})
intSet

# COMMAND ----------

# MAGIC %md
# MAGIC # "Membership Verification" of the "Elements" in a "Set"
# MAGIC * Although, the "<b>Order</b>" is "<b>Arbitrary</b>", "<b>Membership</b>" is a "<b>Fundamental Operation</b>" for "<b>Sets</b>".
# MAGIC * As with "<b>Other Collection Data Types</b>", the "<b>Membership Operation</b>" is "<b>Performed</b>" using the "<b>IN</b>" and "<b>NOT IN</b>" Operators on "<b>Sets</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "Set" Using "IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Present</b>" in a "<b>Set</b>" using "<b>Membership</b>", then "<b>IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>Set</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>Set</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Present" in a "Set" Using the "IN Operator"
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
print("Pomengrate" in myStrSet)

print("Papaya" in myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Membership Verification" of the "Elements" in a "Set" Using "NOT IN Operator"
# MAGIC * To "<b>Verify</b>" that "<b>If</b>" an "<b>Element</b>" is "<b>Not Present</b>" in a "<b>Set</b>" using "<b>Membership</b>", then "<b>NOT IN Operator</b>" is used.
# MAGIC * If the "<b>Element</b>" is "<b>Not Present</b>" in the "<b>Set</b>", "<b>True</b>" is "<b>Returned</b>".
# MAGIC * If the "<b>Element</b>" is "<b>Present</b>" in the "<b>Set</b>", "<b>False</b>" is "<b>Returned</b>".

# COMMAND ----------

# DBTITLE 1,"Verify" If an "Element" is "Not Present" in a "Set" Using the "NOT IN Operator"
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
print("Papaya" not in myStrSet)

print("Pomengrate" not in myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Remove" the "Elements" from a "Set"
# MAGIC * There are "<b>Two Methods</b> available to "<b>Remove</b>" the "<b>Elements</b>" from a "<b>Set</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "Set" Using the "remove ()" Method
# MAGIC * It is possible to "<b>Remove</b>" the "<b>Elements</b>" from a "<b>Set</b>" using the "<b>remove ()</b>" Method.
# MAGIC * The "<b>remove ()</b>" Method "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Value</b>" of the "<b>Set Element</b>" to be "<b>Deleted</b>", and, "<b>Removes</b>" the "<b>Element</b>" from the "<b>Set</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Pomengrate", from the "Set" Using the "remove ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
myStrSet.remove("Pomengrate")
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Value</b>" to be "<b>Deleted</b>", using the "<b>remove ()</b>" Method, from a "<b>Set</b>", is "<b>Not Present</b>" in the "<b>Set</b>", the "<b>KeyError Exception</b>" is "<b>Thrown</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Papaya", Which is "Not Present", in the "Set" Using the "remove ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
myStrSet.remove("Papaya")
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "Set" Using the "discard ()" Method
# MAGIC * It is possible to "<b>Remove</b>" the "<b>Elements</b>" from a "<b>Set</b>" using the "<b>discard ()</b>" Method.
# MAGIC * The "<b>discard ()</b>" Method "<b>Takes</b>" a "<b>Single Parameter</b>", which is the "<b>Value</b>" of the "<b>Set Element</b>" to be "<b>Deleted</b>", and, "<b>Removes</b>" the "<b>Element</b>" from the "<b>Set</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Pomengrate", from the "Set" Using the "discard ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
myStrSet.discard("Pomengrate")
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Value</b>" to be "<b>Deleted</b>", using the "<b>discard ()</b>" Method, from a "<b>Set</b>", is "<b>Not Present</b>" in the "<b>Set</b>", the "<b>discard ()</b>" Method has "<b>No Effect</b>", and, "<b>Neither</b>" does it "<b>Produces</b>" an "<b>Error</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" the "Element", i.e., "Papaya", Which is "Not Present", in the "Set" Using the "discard ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
myStrSet.discard("Papaya")
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" the "Elements" from a "Set" Using the "pop ()" Method
# MAGIC * It is possible to "<b>Remove</b>" any "<b>Random Element</b>" from a "<b>Set</b>" using the "<b>pop ()</b>" Method.
# MAGIC * The "<b>pop ()</b>" Method "<b>Does Not Take</b>" any "<b>Parameter</b>", and, "<b>Returns</b>" the "<b>Deleted Element</b>".

# COMMAND ----------

# DBTITLE 1,"Remove" a "Random Element" from the "Set" Using the "pop ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
deletedSetElement = myStrSet.pop()

print(deletedSetElement)
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Remove" "All" the "Elements" from a "Set" Using the "clear ()" Method
# MAGIC * It is possible to "<b>Remove</b>" "<b>All</b>" the "<b>Elements</b>" from a "<b>Set</b>" using the "<b>clear ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the "Elements" from the "Set" Using the "clear ()" Method
myStrSet = {"Apple", "Banana", "Peach", "Guava", "Apple", "Pomengrate", "Guava", "Watermelon", "Pineapple"}
myStrSet.clear()
print(myStrSet)

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Existing Elements" From a "Python Set" Can Be "Copied" Into "Another Python Set"?

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Copying Sets</b>" is "<b>Shallow</b>" by default, i.e., "<b>Copying</b>" only the "<b>References</b>" to the "<b>Set Elements</b>", and, "<b>Not</b>" the "<b>Set Elements</b>".
# MAGIC * "<b>Assigning</b>" a "<b>New Reference</b>" to the "<b>Reference</b>" of an "<b>Already Existing Python Set</b>" "<b>Never Copies</b>" the "<b>Actual Set Object</b>", but, merely "<b>Copies</b>" the "<b>Reference</b>" to that "<b>Already Existing Python Set Object</b>".

# COMMAND ----------

# DBTITLE 1,"Actual Set Object" is "Not Copied". Only the "Reference" is
intSet = {25, 10, 5, 20, 15}
newIntSet = intSet

# "Both" of the "New Set Reference", and, the "Original Set Reference" have the "Equivalent Value".
print(newIntSet == intSet)

# The "New Set Reference" is "Pointing" to the "Reference" of the "Already Existing Python Set". Hence, "Both" of the "Set References" are "Equal".
print(newIntSet is intSet)

# "Objects" that are "Referred To" by "Both" of the "New Set Reference" and the "Original Set Reference", Present at "Each Index", are the "Same".
print(newIntSet)
print(intSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python Set" into a "New Python Set" Using "copy ()" Method
# MAGIC * The "<b>copy ()</b>" Method  is "<b>Called On</b>" the "<b>Existing Python Set</b>" to "<b>Copy</b>" "<b>All the Elements</b>" of that "<b>Existing Python Set</b>" to a "<b>New Python Set</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python Set" into a "New Python Set" Using "copy ()" Method
intSet = {25, 10, 5, 20, 15}
newIntSet = intSet.copy()

# "Both" of the "New Set Reference", and, the "Original Set Reference" have the "Equivalent Value".
print(newIntSet == intSet)

# The "New Set Reference" is "Pointing" to the "Reference" of the "Already Existing Python Set". Hence, "Both" of the "Set References" are "Equal".
print(newIntSet is intSet)

# "Objects" that are "Referred To" by "Both" of the "New Set Reference" and the "Original Set Reference", Present at "Each Index", are the "Same".
print(newIntSet)
print(intSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Copy" an "Already Existing Python Set" into a "New Python Set" Using "set () Constructor"
# MAGIC * "<b>All the Elements</b>" of an "<b>Existing Python Set</b>" can be "<b>Copied</b>" into a "<b>New Python Set</b>" by "<b>Passing</b>" the "<b>Existing Python Set</b>" to the "<b>set () Constructor</b>".
# MAGIC * Although, it is a "<b>Mattre of Taste</b>", still it is "<b>Preferrable</b>" to use the "<b>set () Constructor</b>" approach to "<b>Copy</b>" "<b>All the Elements</b>" of an "<b>Already Existing Python Set</b>" into a "<b>New Python Set</b>", since, this approach has the "<b>Advantage</b>" of "<b>Working</b>" with "<b>Any Iterable Series</b>" as the "<b>Source</b>", and, "<b>Not Just Sets</b>".

# COMMAND ----------

# DBTITLE 1,"Copy" an "Already Existing Python Set" into a "New Python Set" Using "set () Constructor"
intSet = {25, 10, 5, 20, 15}
newIntSet = set(intSet)

# "Both" of the "New Set Reference", and, the "Original Set Reference" have the "Equivalent Value".
print(newIntSet == intSet)

# The "New Set Reference" is "Pointing" to the "Reference" of the "Already Existing Python Set". Hence, "Both" of the "Set References" are "Equal".
print(newIntSet is intSet)

# "Objects" that are "Referred To" by "Both" of the "New Set Reference" and the "Original Set Reference", Present at "Each Index", are the "Same".
print(newIntSet)
print(intSet)
