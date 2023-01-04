# Databricks notebook source
# MAGIC %md
# MAGIC # Operations On "String"
# MAGIC * Topic: Different "Operations" that Can be "Performed" on the "String" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Operations" on "Strings"
# MAGIC * "<b>Strings</b>" in "<b>Python</b>" are what are called "<b>Sequence Types</b>", which means that "<b>Strings</b>" support certain "<b>Common Operations</b>" for "<b>Querying Sequences</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Indexing" of "Strings"
# MAGIC * It is possible to "<b>Access</b>" the "<b>Individual Characters</b>" of a "<b>String</b>" using the "<b>Square Brackets</b>", i.e., "<b>[]</b>", with an "<b>Integer 0-Based Index</b>".
# MAGIC * "<b>In Contrast</b>" to many "<b>Programming Languages</b>", there is "<b>No Separate Data Type</b>", called the "<b>Character</b>" Data Type in "<b>Python</b>", which is "<b>Distinct</b>" from the "<b>String</b>" Data Type.
# MAGIC * The "<b>Indexing</b>" Operation "<b>Returns</b>" a "<b>Full-Blown String</b>" that can "<b>Contain</b>" a "<b>Single Character Element</b>" or "<b>Multiple Character Elements</b>".

# COMMAND ----------

# DBTITLE 1,Print the "Third Character", i.e., "Index-2" from a "String"
myName = "Oindrila Chakraborty"
print(myName[2])
print(type(myName[2]))

# COMMAND ----------

# DBTITLE 1,Print From the "Third Character", i.e., "Index-2" Till the "Tenth Character", i.e., "Index-9" from a "String"
myName = "Oindrila Chakraborty"
print(myName[2:9])
print(type(myName[2:9]))

# COMMAND ----------

# DBTITLE 1,Print "Every Second Character" From the "Second Character", i.e., "Index-1" Till the "Fifteenth Character", i.e., "Index-14" from a "String"
myName = "Oindrila Chakraborty"
print(myName[1:14:2])
print(type(myName[1:14:2]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Determine" the "Length" of "Strings"
# MAGIC * It is possible to "<b>Determine</b>" the "<b>Length</b>" of a "<b>String</b>" using the "<b>Built-In Python Function</b>", called "<b>len ()</b>".

# COMMAND ----------

# DBTITLE 1,"Determine" the "Length" of a "String" Using the "Built-In Function", i.e., "len ()"
myName = 'oindrila chakraborty'
print(len(myName))

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Concatenation" of the "Strings" Using the "Plus Operator"
# MAGIC * It is possible to "<b>Concatenate</b>" the "<b>String</b>" using the "<b>Plus Operator</b>", i.e., "<b>+</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Strings" Using the "Plus Operator", i.e., "+"
'Oindrila' + "Chakraborty" + 'Bagchi'

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Concatenation" of the "Strings" Using the "Augmented Assignment Operator"
# MAGIC * It is also possible to use the "<b>Augmented Assignment Operator</b>" to "<b>Concatenate</b>" the "<b>Multiple Strings</b>" in "<b>Python</b>".
# MAGIC * Since "<b>Strings</b>" are "<b>Immutable</b>", the "<b>Augmented Assignment Operator</b>" would "<b>Bind</b>" a "<b>New String Object</b>" to the "<b>String Object Reference</b>" on "<b>Each Use</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Strings" Using the "Augmented Assignment Operator"
myName = 'Oindrila'
myName += "Chakraborty"
myName += 'Bagchi'
print(myName)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>Illusion</b>" of "<b>Modifying</b>" the "<b>String Object Reference</b>", i.e., "<b>myName</b>" "<b>In-Place</b>" is "<b>Achievable</b>", because, the "<b>myName</b>" is a "<b>Reference</b>" to a "<b>String Object</b>", and "<b>Not</b>" the "<b>String Object</b>" itself.

# COMMAND ----------

# MAGIC %md
# MAGIC # "Methods" of "Strings"
# MAGIC * "<b>String Objects</b>" also support a wide variety of "<b>Operations</b>" that are "<b>Implemented</b>" as "<b>Methods</b>".
# MAGIC * To "<b>Call</b>" the "<b>Methods</b>" on the "<b>Objects</b>" in "<b>Python</b>", the "<b>Dot</b>", i.e., "<b>.</b>" is used "<b>After</b>" the "<b>Object Name</b>", and, "<b>Before</b>" the "<b>Method Name</b>".
# MAGIC * "<b>Methods</b>" are "<b>Functions</b>". So, the "<b>Parentheses</b>" must be used to "<b>Indicate</b>" that the "<b>Method</b>" should be "<b>Called</b>".
# MAGIC * Since "<b>Strings</b>" are "<b>Immutable</b>", any "<b>Method</b>" that is "<b>Applied</b>" on a "<b>String Object</b>", "<b>Does Not Modify</b>" the "<b>String Object In-Place</b>". Rather "<b>Returns</b>" a "<b>New String Object</b>".

# COMMAND ----------

# DBTITLE 1,"List" the "String Methods" Using the "help ()" Function
help(str)

# COMMAND ----------

# DBTITLE 1,"Capitalize" the "First Letter" of a "String"
myName = 'oindrila chakraborty'
myCapitalName = myName.capitalize()

print(myName)
print(myCapitalName)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Concatenation" of the "Strings" Using the "join ()" Method
# MAGIC * The "<b>join ()</b>" Method in the "<b>String</b>" Class "<b>Takes</b>" the "<b>Multiple String Objects</b>" to be "<b>Concatenated</b>" as an "<b>Argument</b>" as a "<b>Collection</b>" Data Type, like - "<b>List</b>", "<b>Tuple</b>" etc., and, "<b>Produces</b>" a "<b>New String Object</b>", by "<b>Inserting</b>" a "<b>Separator</b>" between "<b>Each</b>" of the "<b>Input String Objects</b>".
# MAGIC * An interesting aspect of the "<b>join ()</b>" Method is "<b>How</b>" the "<b>Separator</b>" is specified. The "<b>join ()</b>" Method is "<b>Called</b>" on the "<b>Separator String</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why the "join ()" Method is the "Preferred Approach" for "String Concatenation"?
# MAGIC * The "<b>join ()</b>" Method should be "<b>Preferred</b>" for "<b>Concatenating</b>", i.e., "<b>Joining</b>" the "<b>Large Numbers</b>" of "<b>Strings</b>", because, the "<b>join ()</b>" Method is substantially "<b>More Efficient</b>", because, the "<b>Concatenation</b>" of "<b>Strings</b>" with the "<b>Plus/Addition Operator</b>", or, with the "<b>Augmented Assignment Operator</b>" can "<b>Lead</b>" to the "<b>Generation</b>" of "<b>Large Numbers</b>" of "<b>Temporary String Objects</b>" with "<b>Consequent Costs</b>" for "<b>Memory</b>", "<b>Allocation</b>" and "<b>Copies</b>".

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Strings" Using the "join ()" Method of "String" Class By Passing "Multiple Strings" as "List"
namesList = ['Oindrila', 'Chakraborty', 'Bagchi']
myName = " ".join(namesList)
print(myName)

# COMMAND ----------

# DBTITLE 1,"Concatenate" "Multiple Strings" Using the "join ()" Method of "String" Class By Passing "Multiple Strings" as "Tuple"
namesTuple = ('Oindrila', 'Chakraborty', 'Bagchi')
myName = " ".join(namesTuple)
print(myName)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Splitting" of the "Strings" Using the "split ()" Method
# MAGIC * The "<b>split ()</b>" Method in the "<b>String</b>" Class is "<b>Called</b>" on a "<b>String Object</b>", and, "<b>Produces</b>" "<b>Multiple New String Objects</b>", by "<b>Splitting</b>" the "<b>Input String Object</b>" based on an "<b>White Space String</b>" as a "<b>Separator</b>", by default.

# COMMAND ----------

# DBTITLE 1,"Split" the "Content" of a "String"
myName = 'Oindrila Chakraborty Bagchi'
print(myName.split())

# COMMAND ----------

# MAGIC %md
# MAGIC ####  "Splitting" of the "Strings" Using the "split ()" Method By Passing "Optional Argument"
# MAGIC * It is also possible to "<b>Split</b>" a "<b>String</b>" using any other "<b>Separator</b>" that is "<b>Not</b>" the "<b>Default Separator</b>", i.e., the "<b>White Space String</b>" by "<b>Passing</b>" the "<b>Separator Value</b>" in the "<b>split ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,"Split" the "Content" of a "String" Using ";" As "Separator"
myName = 'Oindrila;Chakraborty;Bagchi'
print(myName.split(';'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Partitioning" a "String" Using the "partition ()" Method
# MAGIC * The "<b>partition ()</b>" Method in the "<b>String</b>" Class is "<b>Called</b>" on a "<b>String Object</b>", and, "<b>Takes</b>" a "<b>Sepaeator String</b>" as an "<b>Argument</b>", and, then "<b>Divides</b>" the "<b>Input String</b>" into "<b>Three Sections</b>" -
# MAGIC <br><b>1</b>. The part "<b>Before the Separator</b>"
# MAGIC <br><b>2</b>. The "<b>Separator</b>" itself
# MAGIC <br><b>3</b>. The part "<b>After the Separator</b>"
# MAGIC * "<b>String Partitioning</b>" using the "<b>partition ()</b>" Method "<b>Returns</b>" a "<b>Tuple</b>". Hence, this Method is "<b>Commonly Used</b>" in "<b>Conjunction With</b>" the "<b>Tuple Unpacking</b>".

# COMMAND ----------

# DBTITLE 1,"Partition" the "Content" of a "String"
fullName = 'Oindrila Chakraborty Bagchi'
firstName, separator, lastName = fullName.partition(" ")
print(firstName)
print(separator)
print(lastName)

# COMMAND ----------

# MAGIC %md
# MAGIC * Often the "<b>Developers</b>" are "<b>Not Interested</b>" in "<b>Capturing</b>" the "<b>Separator Value</b>". So, to "<b>Capture</b>" the "<b>Separator Value</b>", most of the times, the "<b>Underscopre Variable</b>", i.e., "<b>_</b>" is used.
# MAGIC * This is "<b>Not Treated in a Special Way</b>" by "<b>Python</b>", but, there is an "<b>Unwritten Convention</b>" that the "<b>Underscopre Variable</b>" is used for "<b>Unused</b>", or, "<b>Dummy</b>" Values. This "<b>Convention</b>" is supported by many "<b>Python-Aware Development Tools</b>", which will "<b>Supress</b>" the "<b>Unused Variable Warnings</b>" for "<b>Underscopre Variable</b>".

# COMMAND ----------

# DBTITLE 1,"Partition" the "Content" of a "String" With "Underscore Variable" for "Separator"
fullName = 'Oindrila Chakraborty Bagchi'
firstName, _, lastName = fullName.partition(" ")
print(firstName)
print(_)
print(lastName)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Formatting" of a "String" Using "format ()" Method
# MAGIC * One of the most interesting and frquently used "<b>String Method</b>" is "<b>format ()</b>".
# MAGIC * The "<b>format ()</b>" Method "<b>Supersedes</b>", although "<b>Does Not Replace</b>" the "<b>String Interpolation Technique</b>" that was used in the "<b>Older Versions</b>" of "<b>Python</b>".
# MAGIC * The "<b>format ()</b>" Method can be "<b>Called</b>" on any "<b>String</b>" that "<b>Contains</b>" the so-called "<b>Replacement Fields</b>", which are "<b>Surrounder By</b>" the "<b>Curly Braces</b>".
# MAGIC * The "<b>Objects</b>" that are provided as the "<b>Arguments</b>" to the "<b>format ()</b>" Method are "<b>Converted</b>" to "<b>Strings</b>" and used to "<b>Populate</b>" the "<b>Replacement Fields</b>"

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Using "format ()" Method With the "Positional Arguments"
# MAGIC * In case of the "<b>format ()</b>" Method with the "<b>Positional Arguments</b>", the "<b>Replacement Field Numbers</b>", which are "<b>Positive Integer Numbers</b>", can be used to "<b>Match Up</b>" with the "<b>Positional Arguments</b>" that are sent to the to "<b>format ()</b>" Method.
# MAGIC * In the following example, the "<b>Positional Arguments</b>" that are sent to the "<b>format ()</b>" Method are - 
# MAGIC <br><b>1</b>. A "<b>String</b>" with the "<b>Value</b>" as "<b>Oindrila</b>"
# MAGIC <br><b>2</b>. An "<b>Integer</b>" with the "<b>Value</b>" as "<b>34</b>"
# MAGIC <br>These "<b>Positional Arguments</b>" will be "<b>Inserted</b>" into the "<b>String</b>" to be "<b>Formatted</b>".
# MAGIC * For the following example, the "<b>Replacement Field Numbers</b>" are "<b>Specified</b>" by "<b>0</b>", and, "<b>1</b>", which are "<b>Matched Up</b>" with the "<b>Positional Arguments</b>", i.e., "<b>Oindrila</b>", and, "<b>34</b>" respectively, which are sent to the to "<b>format ()</b>" Method, and, "<b>Each</b>" of the "<b>Positional Arguments</b>" are "<b>Converted</b>" to "<b>Strings</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Positional Argument"
'My name is {0}. My age is {1}'.format("Oindrila", 34)

# COMMAND ----------

# MAGIC %md
# MAGIC * In case of the "<b>format ()</b>" Method with the "<b>Positional Arguments</b>", the "<b>Replacement Field Numbers</b>" may be used "<b>More Than Once</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Positional Argument" Having "Replacement Field Numbers" Used "More Than Once"
'The age of {0} is {1}. {0}\'s birthday is on {2}'.format("Oindrila", 34, '2nd September')

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>Replacement Fields</b>" are used "<b>Exactly Once</b>", and, in the "<b>Same Order</b>" as the "<b>Positional Arguments</b>" that are sent to the to "<b>format ()</b>" Method, the "<b>Numbers</b>" from the "<b>Replacement Fields</b>" can be "<b>Ommitted</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Positional Argument" Without Using "Replacement Field Numbers"
'My name is {}. My age is {}'.format("Oindrila", 34)

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Specify</b>" the "<b>Replacement Field Numbers</b>" in the "<b>String</b>" to be "<b>Formatted</b>" in a "<b>Different Order</b>" than the "<b>Order</b>" in which the "<b>Positional Arguments</b>" are sent to the "<b>format ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,Specify the "Replacement Field Numbers" in "Different Order"
'My age is {1}. My name is {0}'.format("Oindrila", 34)

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Using "format ()" Method With the "Keyword Arguments"
# MAGIC * In case of the "<b>format ()</b>" Method with the "<b>Keyword Arguments</b>", the "<b>Named Replacement Fields</b>" can be used, "<b>Instead Of</b>" the "<b>Ordinals</b>".
# MAGIC * In the following example, the "<b>Keyword Arguments</b>" that are sent to the "<b>format ()</b>" Method are - 
# MAGIC <br><b>1</b>. <b>name</b>: A "<b>String</b>" with the "<b>Value</b>" as "<b>Oindrila</b>"
# MAGIC <br><b>2</b>. <b>age</b>: An "<b>Integer</b>" with the "<b>Value</b>" as "<b>34</b>"
# MAGIC <br>These "<b>Keyword Arguments</b>" will be "<b>Inserted</b>" into the "<b>String</b>" to be "<b>Formatted</b>" to the corresponding "<b>Named Replacement Fields</b>".
# MAGIC * For the following example, the "<b>Named Replacement Fields</b>" are "<b>Specified</b>" by "<b>name</b>", and, "<b>age</b>", which are "<b>Matched Up</b>" with the "<b>Keyword Arguments</b>", i.e., "<b>name</b>", and, "<b>age</b>" respectively, which are sent to the to "<b>format ()</b>" Method, and, "<b>Each</b>" of the "<b>Keyword Arguments</b>" are "<b>Converted</b>" to "<b>Strings</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Keyword Argument"
'My name is {name}. My age is {age}'.format(name = "Oindrila", age = 34)

# COMMAND ----------

# MAGIC %md
# MAGIC * In case of the "<b>format ()</b>" Method with the "<b>Keyword Arguments</b>", the "<b>Named Replacement Fields</b>" may be used "<b>More Than Once</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Keyword Argument" Having "Named Replacement Fields" Used "More Than Once"
'The age of {name} is {age}. {name}\'s birthday is on {birthday}'.format(name = "Oindrila", age = 34, birthday = '2nd September')

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Specify</b>" the "<b>Named Replacement Fields</b>" in the "<b>String</b>" to be "<b>Formatted</b>" in a "<b>Different Order</b>" than the "<b>Order</b>" in which the "<b>Keyword Arguments</b>" are sent to the "<b>format ()</b>" Method.

# COMMAND ----------

# DBTITLE 1,Specify the "Named Replacement Fields" in "Different Order"
'My age is {age}. My name is {name}'.format(name = "Oindrila", age = 34)

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to provide "<b>Index</b>" into the "<b>Sequences</b>" using a "<b>Square Brackets</b>", i.e., "<b>[]</b>", inside the "<b>Named Replacement Fields</b>" when a "<b>Collection Data Type</b>" is sent as a "<b>Keyword Argument</b>" to the "<b>format ()</b>" Method.
# MAGIC * To "<b>Use</b>" a "<b>Value</b>" that is "<b>Present</b>" at a particular "<b>Index</b>" in the "<b>Keyword Argument</b>", i.e., a "<b>Collection Data Type</b>", in the "<b>String</b>" to be "<b>Formatted</b>", the corresponding "<b>Index</b>" can be provided with the "<b>Named Replacement Field</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Keyword Argument" as "Collection" Data Type
'The age of {personTuple[0]} is {personTuple[1]}. {personTuple[0]}\'s birthday is on {personTuple[2]}'.format(personTuple = ("Oindrila", 34, '2nd September'))

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Access</b>" the "<b>Object Attributes</b>" from the "<b>String</b>" to be "<b>Formatted</b>", i.e., from within the "<b>Named Replacement Fields</b>" in the "<b>String</b>" to be "<b>Formatted</b>", where an "<b>Entire Object</b>" can be "<b>Passed</b>" as the "<b>Keyword Argument</b>" to the "<b>format ()</b>" Method.
# MAGIC * A "<b>Python Module</b>" can be "<b>Passed</b>" as a "<b>Keyword Argument</b>" to the "<b>format ()</b>" Method, because, "<b>Python Modules</b>" are "<b>Objects</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Keyword Argument" as "Object"
import math

"The value of 'pi' is : {mathVal.pi}".format(mathVal = math)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>format ()</b>" Method gives a lot of "<b>Control</b>" over the "<b>Field Alignment</b>" and "<b>Floating-Point Formatting</b>".

# COMMAND ----------

# DBTITLE 1,Usage of "format ()" Method With "Keyword Argument" to "Display" a "Float Value" with "Three Decimal Places"
import math

"The value of 'pi' is : {mathVal.pi:.3f}".format(mathVal = math)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Issues" of "Formatting" a "String" Using "format ()" Method
# MAGIC * While the "<b>format ()</b>" Method is "<b>Quite Powerful</b>" and generally "<b>Preferrable</b>" to its "<b>Predecessors</b>", it could be "<b>Quite Verbose</b>", even for the "<b>Relatively Simple Cases</b>".
# MAGIC * In case of "<b>Complex String Interpolation</b>", the "<b>Variables</b>", containing "<b>Values</b>", need to be kept "<b>In-Place</b>" for "<b>Readability</b>" and "<b>Maintainability</b>". That means, it is the "<b>Developer's Responsibility</b>" to "<b>Keep Track</b>" and "<b>Maintain</b>"  which "<b>Variable</b>" will be used to "<b>Pass</b>" the "<b>Value</b>" to which "<b>Keyword Argument</b>" in the "<b>format ()</b>" Method.
# MAGIC * Example - if an "<b>Integer Expression</b>" is "<b>Assigned</b>" to a "<b>Variable</b>", and then, if the "<b>Calculated Value</b>" of that "<b>Variable</b>" is "<b>Interpolated</b>" into a "<b>String</b>" using the "<b>format ()</b>" Method's "<b>Keyword Argument</b>" Feature, it is the "<b>Developer's Responsibility</b>" to provide the "<b>Name</b>" of the "<b>Variable</b>" to the "<b>Proper</b>" "<b>Keyword Argument</b>" in the "<b>format ()</b>" Method..

# COMMAND ----------

# DBTITLE 1,"Variable" Needs to be "Kept In Check" by "Developer" to Use in "Keyword Argument"
myValue = 10 * 4
'The value is {value}'.format(value = myValue)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  "Formatting" of a "String" Using "Literal String Interpolation", or, Commonly Known as "f-strings"
# MAGIC * "<b>F-Strings</b>" are "<b>Available</b>" in "<b>Python 3.6</b>", and, later.
# MAGIC * "<b>F-Strings</b>" provide a way to "<b>Embed</b>" the "<b>Expressions</b>" inside the "<b>Literal Strings</b>" using a "<b>Minimal Syntax</b>".
# MAGIC * An "<b>F-String</b>" is like a "<b>Normal Literal String</b>", except that, it is "<b>Prefixed</b>" with the "<b>Letter f</b>".
# MAGIC * Inside the"<b>Literal Strings</b>", "<b>Python Expressions</b>" can be "<b>Embedded</b>" inside the "<b>Curly Braces</b>", i.e., "<b>{}</b>", and, the "<b>Results</b>" of these "<b>Expressions</b>" will be "<b>Inserted</b>" into the "<b>Literal String</b>" at "<b>Run Time</b>".

# COMMAND ----------

# DBTITLE 1,Insert "Integer Expression" Into "Literal String" Using "F-String" Approach
myValue = 10 * 4
'The value is {myValue}'

# COMMAND ----------

# MAGIC %md
# MAGIC * Because the "<b>F-Strings</b>" allow to use any "<b>Python Expression</b>", the "<b>Developers</b>" are "<b>Not Limited</b>" to use only "<b>Simple Named References</b>", but also can "<b>Call Functions</b>" using the "<b>F-Strings</b>".

# COMMAND ----------

# DBTITLE 1,"Call Functions" Using the "F-String" Approach
import datetime

f'The Current Time is : {datetime.datetime.now().isoformat()}.'

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Access</b>" the "<b>Object Attributes</b>" from within a "<b>Literal String</b>" by  "<b>Accessing</b>" the "<b>Object Attributes</b>" from within the "<b>Curly Braces</b>" of the "<b>F-String</b>".

# COMMAND ----------

# DBTITLE 1,Display the "Object Attributes" Using the "F-String" Approach
import math

f"The value of 'pi' is : {math.pi}"

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>F-String</b>" approach supports the "<b>Floating-Point Formatting</b>".

# COMMAND ----------

# DBTITLE 1,"Display" a "Float Value" with "Three Decimal Places" Using the "F-String" Approach
import math

f"The value of 'pi' is : {math.pi:.3f}"

# COMMAND ----------

# MAGIC %md
# MAGIC * In the "<b>F-String</b>" approach, if an "<b>!r</b>" is used after the "<b>String Expression</b>", the "<b>REPR Representation</b>" of the "<b>Value</b>" will be "<b>Inserted</b>" into the "<b>String Expression</b>".

# COMMAND ----------

import datetime

f'The Current Time is : {datetime.datetime.now().isoformat()!r}.'
