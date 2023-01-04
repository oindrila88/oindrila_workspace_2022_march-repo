# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Comprehension"
# MAGIC * Topic: Introduction to "Comprehension" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Iteration"?
# MAGIC * A "<b>Central Abstraction</b>" in "<b>Python</b>" is the "<b>Notion</b>" of an "<b>Iterable</b>", i.e., an "<b>Object</b>" from which it is possible to "<b>Fetch</b>" a "<b>Sequence</b>" of "<b>Other Objects</b>".
# MAGIC * The "<b>Act</b>" of "<b>Fetching</b>" a "<b>Sequence</b>" from an "<b>Iterable</b>" is known as "<b>Iteration</b>".
# MAGIC * "<b>Iteration</b>" is "<b>Encountered</b>" in the form of "<b>Loops</b>", such as - "<b>for Loop</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Comprehension"?
# MAGIC * "<b>Comprehensions</b>" in "<b>Python</b>" are "<b>Concise Syntax</b>" for "<b>Describing</b>" the "<b>Lists</b>", "<b>Sets</b>", or, "<b>Dictionaries</b>" in a "<b>Declarative</b>", or, "<b>Functional</b>" <b>Style</b>.
# MAGIC * The "<b>Shorthand</b>" for "<b>Comprehensions</b>" is "<b>Readable</b>", and, "<b>Expressive</b>", meaning that the "<b>Comprehensions</b>" are "<b>Very Effective</b>" at "<b>Communicating Intent</b>" to "<b>Human Readers</b>".
# MAGIC * Some "<b>Comprehensions</b>" almost "<b>Read</b>" like "<b>Natural Language</b>", making those "<b>Nicely Self-Documenting</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "List Comprehension"
# MAGIC * The "<b>List Comprehension</b>" is "<b>Enclosed</b>" in "<b>Square Brackets</b>", just "<b>Like</b>" a "<b>Literal List</b>".
# MAGIC * But, "<b>Instead of</b>" the "<b>Literal List Elements</b>", it contains a "<b>Fragment</b>" of "<b>Declrative Code</b>", which "<b>Describes</b>" the "<b>Logic</b>" to "<b>Construct</b>" the "<b>Elements</b>" of the "<b>Newly Constructed List</b>".
# MAGIC * The "<b>General Form</b>" of the "<b>List Comprehension</b>" is as follows -
# MAGIC   * An "<b>Opening Square Bracket</b>"
# MAGIC   * An "<b>Expression</b>" of "<b>Declrative Code</b>"
# MAGIC   * A "<b>Statement</b>" <b>Binding</b> a "<b>Name</b>" to "<b>Successive Elements</b>" of an "<b>Iterable</b>"
# MAGIC   * A "<b>Closing Square Bracket</b>"
# MAGIC * For "<b>Each Item</b>" in the "<b>Iterable</b>" on the "<b>Right</b>", the "<b>Expression</b>" is "<b>Evaluated</b>" on the "<b>Left</b>", which is "<b>Almost Always</b>, but, "<b>Not Necessarily</b>" in terms of the "<b>Item</b>", and, that is "<b>Used</b>" as the "<b>Next Element</b>" of the "<b>List</b>" being "<b>Constructed</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New List" From an "Existing List" Using "List Comprehension"
# Create a "List" of "Strings" Containing "Names"
listOfNames = ["Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"]

# Create a "New List" From "List" of "Strings" Containing "Names" Using "List Comprehension"
[len(name) for name in listOfNames]

# COMMAND ----------

# MAGIC %md
# MAGIC * In the above example of "<b>List Comprehension</b>", the "<b>Declrative Code</b>" is "<b>Described</b>" as "<b>len(word)</b>", which is "<b>Looped Over</b>" for "<b>All</b>" the "<b>Elements</b>" of the "<b>List</b>".
# MAGIC * The "<b>New List</b>" is "<b>Formed</b>" by "<b>Binding</b>" "<b>name</b>" to "<b>Each Value</b>" in the <b>List</b>, i.e., "<b>listOfNames</b>", and, in turn, "<b>Evaluating</b>" the "<b>len(name)</b>" to "<b>Create</b>" a "<b>New Value</b>". "<b>Each</b>" of these "<b>New Values</b>" becomes an "<b>Element</b>" in the "<b>Newly Constructed List</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "List Comprehension" from "Iterables" Other Than "Lists"
# MAGIC * In "<b>List Comprehension</b>", the "<b>Source Object</b>", "<b>Over</b>" which it is to be "<b>Iterated</b>" "<b>Does Not</b>" need to be a "<b>List</b>". It can be "<b>Any Object</b>", which "<b>Implements</b>" the "<b>Iterable Protocol</b>", such as a "<b>Tuple</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New List" From an "Existing Tuple" Using "List Comprehension"
# Create a "Tuple" of "Strings" Containing "Names"
tupleOfNames = ("Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura")

# Create a "New List" From "Tuple" of "Strings" Containing "Names" Using "List Comprehension"
[len(name) for name in tupleOfNames]

# COMMAND ----------

# DBTITLE 1,Create a "New List" From an "Existing Set" Using "List Comprehension"
# Create a "Set" of "Strings" Containing "Names"
setOfNames = {"Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"}

# Create a "New List" From "Set" of "Strings" Containing "Names" Using "List Comprehension"
[len(name) for name in setOfNames]

# COMMAND ----------

# MAGIC %md
# MAGIC ### The "Expression" of "Declrative Code" in "List Comprehension"
# MAGIC * The "<b>Expression</b>" of "<b>Declrative Code</b>" that is "<b>Producing</b>" the "<b>New List's Elements</b>" can be "<b>Any Python Expression</b>".
# MAGIC * In the following example,  the "<b>Number</b>" of "<b>Decimal Digits</b>" in "<b>Each</b>" of the "<b>First 20 Factorials</b>" are "<b>Fetched</b>" Using "<b>Range</b>" to "<b>Generate</b>" the "<b>Source</b>" of "<b>Sequence</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New List" From a "Sequence" that is "Generated" From "range ()" Using "List Comprehension"
# Import the "math" Module
from math import factorial

# "Calculate" the "Length" of the "Decimal String Representation" of the "Factorial" of "Each Integer", from "0" to "19".
listOfFacts = [len(str(factorial(num))) for num in range(20)]
print(listOfFacts)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Equivalent Syntax" of "List Comprehension"
# MAGIC * The "<b>List Comprehension</b>" is the "<b>Declrative Equivalent</b>" of the following "<b>Imperative Code</b>".
# MAGIC * First, an "<b>Empty List</b>" is "<b>Created</b>", e.g., "<b>listOfNamesWithNameLengths</b>".
# MAGIC * Second, a "<b>for</b>" <b>Loop</b> is "<b>Used</b>" to "<b>Iterate Over</b>" the "<b>Existing List</b>", i.e., "<b>listOfNames</b>" to "<b>Bind</b>" "<b>Each Element</b>" of the "<b>Existing List</b>", i.e., "<b>listOfNames</b>" to a "<b>Variable</b>", i.e., "<b>name</b>" for "<b>Each Iteration</b>".
# MAGIC * Third, for "<b>Each Iteration</b>", the "<b>Length</b>" of the "<b>Variable</b>", i.e., "<b>name</b>" is "<b>Calculated</b>", and, is "<b>Appended</b>" to the "<b>Empty List</b>", i.e., "<b>listOfNamesWithNameLengths</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New List" From an "Existing List" Using the "Equivalent Syntax"
# Create a "List" of "Strings" Containing "Names"
listOfNames = ["Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"]

# Create a "New List" From "List" of "Strings" Containing "Names" Using the "Equivalent Syntax"
# Create an "Empty List"
listOfNamesWithNameLengths = []

# "Loop Through" the "Existing List", i.e., "listOfNames" to "Append" the "Length" of "Each Element" of the "Existing List", i.e., "listOfNames" to the "Created Empty List", i.e., "listOfNamesWithNameLengths"
for name in listOfNames:
  listOfNamesWithNameLengths.append(len(name))
  
print(listOfNamesWithNameLengths)

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "Set Comprehension"
# MAGIC * The "<b>Set Comprehension</b>" is "<b>Enclosed</b>" in "<b>Curly Braces</b>", just "<b>Like</b>" a "<b>Literal Set</b>".
# MAGIC * But, "<b>Instead of</b>" the "<b>Literal Set Elements</b>", it contains a "<b>Fragment</b>" of "<b>Declrative Code</b>", which "<b>Describes</b>" the "<b>Logic</b>" to "<b>Construct</b>" the "<b>Elements</b>" of the "<b>Newly Constructed Set</b>".
# MAGIC * The "<b>General Form</b>" of the "<b>Set Comprehension</b>" is as follows -
# MAGIC   * An "<b>Opening Curly Braces</b>"
# MAGIC   * An "<b>Expression</b>" of "<b>Declrative Code</b>"
# MAGIC   * A "<b>Statement</b>" <b>Binding</b> a "<b>Name</b>" to "<b>Successive Elements</b>" of an "<b>Iterable</b>"
# MAGIC   * A "<b>Closing Curly Braces</b>"
# MAGIC * For "<b>Each Item</b>" in the "<b>Iterable</b>" on the "<b>Right</b>", the "<b>Expression</b>" is "<b>Evaluated</b>" on the "<b>Left</b>", which is "<b>Almost Always</b>, but, "<b>Not Necessarily</b>" in terms of the "<b>Item</b>", and, that is "<b>Used</b>" as the "<b>Next Element</b>" of the "<b>Set</b>" being "<b>Constructed</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Set" From an "Existing Set" Using "Set Comprehension"
# Create a "Set" of "Strings" Containing "Names"
setOfNames = {"Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"}

# Create a "New Set" From "List" of "Strings" Containing "Names" Using "Set Comprehension"
{len(name) for name in setOfNames}

# COMMAND ----------

# MAGIC %md
# MAGIC * Since, there are "<b>Multiple Names</b>" in the <b>Set</b>, i.e., "<b>setOfNames</b>", having "<b>Same Number of Letters</b>", by "<b>Building</b>" a "<b>Set</b>", the "<b>Duplicates</b>" can be "<b>Eliminated</b>".
# MAGIC * The "<b>Resulting Set</b>" is "<b>Not Necessarily Stored</b>" in a "<b>Meaningful Order</b>", since "<b>Sets</b>" are "<b>Unordered Containers</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Set Comprehension" from "Iterables" Other Than "Sets"
# MAGIC * In "<b>Set Comprehension</b>", the "<b>Source Object</b>", "<b>Over</b>" which it is to be "<b>Iterated</b>" "<b>Does Not</b>" need to be a "<b>Set</b>". It can be "<b>Any Object</b>", which "<b>Implements</b>" the "<b>Iterable Protocol</b>", such as a "<b>Tuple</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Set" From an "Existing Tuple" Using "Set Comprehension"
# Create a "Tuple" of "Strings" Containing "Names"
tupleOfNames = ("Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura")

# Create a "New Set" From "Tuple" of "Strings" Containing "Names" Using "Set Comprehension"
{len(name) for name in tupleOfNames}

# COMMAND ----------

# DBTITLE 1,Create a "New Set" From an "Existing List" Using "Set Comprehension"
# Create a "List" of "Strings" Containing "Names"
listOfNames = ["Oindrila", "Soumyajyoti", "Oishi", "Souvik", "Anamika", "Swaralip", "Soumyajit", "Sagarneel", "Abhirup", "Madhura"]

# Create a "New Set" From "List" of "Strings" Containing "Names" Using "Set Comprehension"
{len(name) for name in listOfNames}

# COMMAND ----------

# MAGIC %md
# MAGIC ### The "Expression" of "Declrative Code" in "Set Comprehension"
# MAGIC * The "<b>Expression</b>" of "<b>Declrative Code</b>" that is "<b>Producing</b>" the "<b>New Set's Elements</b>" can be "<b>Any Python Expression</b>".
# MAGIC * In the following example,  the "<b>Number</b>" of "<b>Decimal Digits</b>" in "<b>Each</b>" of the "<b>First 20 Factorials</b>" are "<b>Fetched</b>" Using "<b>Range</b>" to "<b>Generate</b>" the "<b>Source</b>" of "<b>Sequence</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Set" From a "Sequence" that is "Generated" From "range ()" Using "Set Comprehension"
# Import the "math" Module
from math import factorial

# "Calculate" the "Length" of the "Decimal String Representation" of the "Factorial" of "Each Integer", from "0" to "19".
setOfFacts = {len(str(factorial(num))) for num in range(20)}
print(setOfFacts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "Dictionary Comprehension"
# MAGIC * The "<b>Dictionary Comprehension</b>" is also "<b>Enclosed</b>" in "<b>Curly Braces</b>", and, is "<b>Distinguished</b>" from the "<b>Set Comprehension</b>" by the fact that "<b>Two Colon Separated Expressions</b>" are "<b>Provided</b>" for the "<b>Key</b>", and,the "<b>Value</b>", which will be "<b>Evaluated In Tandem</b>" for "<b>Each New Item</b>".
# MAGIC * The "<b>General Form</b>" of the "<b>Dictionary Comprehension</b>" is as follows -
# MAGIC   * An "<b>Opening Curly Braces</b>"
# MAGIC   * "<b>Two Colon Separated Expressions</b>" of "<b>Declrative Code</b>"
# MAGIC   * A "<b>Statement</b>" <b>Binding</b> a "<b>Name</b>" to "<b>Successive Elements</b>" of an "<b>Iterable</b>"
# MAGIC   * A "<b>Closing Curly Braces</b>"
# MAGIC * "<b>Dictionary Comprehensions</b>" "<b>Do Not Work Directly</b>" on "<b>Dictionary Sources</b>", because, "<b>Iterating Over</b>" a "<b>Dictionary</b>" only "<b>Yields</b>" the "<b>Keys</b>". So, to "<b>Fetch</b>" both the "<b>Keys</b>", and, the "<b>Values</b>", the "<b>item ()</b>" <b>Method</b> should be used. Then the "<b>Tuple Unpacking</b>" needs to be used to "<b>Access</b>" the "<b>Key</b>", and, the "<b>Value</b>" separately.
# MAGIC * One "<b>Nice Use</b>" of a "<b>Dictionary Comprehension</b>" is to "<b>Invert</b>" a "<b>Dictionary</b>" to "<b>Perform</b>" an "<b>Efficient Lookup</b>" in the "<b>Opposite Direction</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
# Create a "Dictionary" Having "Country Name" as the "Key", and, the Corresponding "Capital" as the "Value"
dictCountryToCapital = {
  "India":"New Delhi",
  "Bangladesh":"Dhaka",
  "Japan":"Tokyo",
  "Indonesia":"Jakarta",
  "Thailand":"Bangkok",
  "China":"Beijing",
  "South Korea":"Seoul",
  "Pakistan":"Islamabad",
  "North Korea":"Pyongyang",
  "Vietnam":"Hanoi",
  "Myanmar":"Naypyidaw"
}

dictCapitalToCountry = {capital : country for country, capital in dictCountryToCapital.items()}
print(dictCapitalToCountry)

# COMMAND ----------

# MAGIC %md
# MAGIC * In the above example of "<b>Dictionary Comprehension</b>", the <b>Dictionary</b>, i.e., "<b>dictCountryToCapital</b>" "<b>Maps</b>" the "<b>Countries</b>" to the Corresponding "<b>Capital Cities</b>".
# MAGIC * To "<b>Invert</b>" a "<b>Dictionary</b>", the "<b>Capital Cities</b>" are "<b>Used</b>" as the "<b>Keys</b>" by "<b>Putting</b>" the "<b>Capital Cities</b>" on the "<b>Left</b>" of the "<b>Colon</b>", and, the "<b>Countries</b>" are "<b>Used</b>" as the "<b>Values</b>" by "<b>Putting</b>" the "<b>Countries</b>" on the "<b>Right</b>" of the "<b>Colon</b>".
# MAGIC * The "<b>Tuple</b>", containing the <b>Country</b>", and, the Corresponding "<b>Capital City</b>" are "<b>Fetched</b>" by "<b>Looping Over</b>" the "<b>dictCountryToCapital.items()</b>" <b>Method</b>.

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Dictionary Comprehension" with "Identical Keys"
# MAGIC * If the "<b>Dictionary Comprehension</b>" "<b>Produces</b>" some "<b>Identical Keys</b>", then "<b>Later Keys</b>" will "<b>Overwrite</b>" the "<b>Earlier Keys</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
# Create a "Dictionary" Having "Country Name" as the "Key", and, the Corresponding "Capital" as the "Value"
dictCountryToCity = {
  "India":"New Delhi",
  "Japan":"Tokyo",
  "South Korea":"Seoul",
  "India":"Kolkata",
  "Japan":"Ginza",
  "South Korea":"Busan",
  "India":"Patna"
}

dictCityToCountry = {city : country for country, city in dictCountryToCity.items()}
print(dictCityToCountry)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Complex Comprehension Expressions" Can be Used as "Separate Functions"
# MAGIC * There is "<b>No Limit</b>" to the "<b>Complexity</b>" of the "<b>Expression</b>" of "<b>Declrative Code</b>" to be "<b>Used</b>" in "<b>Any</b>" of the "<b>Comprehensions</b>", but, the "<b>Excessive Complexities</b>" should be "<b>Avoided</b>".
# MAGIC * The "<b>Complex Expressions</b>" should be "<b>Extracted</b>" into "<b>Separate Functions</b>" to "<b>Preserve</b>" the "<b>Readability</b>".

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
# Create a "Dictionary" Having Name" as the "Key", and, the Corresponding "Person's Age" as the "Value"
dictPerson = {
  "Oindrila": 34,
  "Soumyajyoti": 35,
  "Premanshu": 66,
  "Rama": 61,
  "Kasturi": 29
}

# COMMAND ----------

# DBTITLE 1,Create a "Function" to "Check" If a "Person" is "Senior Citizen"
def isSeniorCitizen (age):
  if age >= 60:
    return True
  else:
    return False;

# COMMAND ----------

# DBTITLE 1,Create a "New Dictionary" From an "Existing Dictionary" Using "Dictionary Comprehension"
dictPersonWithSeniorCitizen = {person : isSeniorCitizen(age) for person, age in dictPerson.items()}
print(dictPersonWithSeniorCitizen)
