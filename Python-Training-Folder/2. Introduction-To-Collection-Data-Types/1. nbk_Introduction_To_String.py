# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "String"
# MAGIC * Topic: Introduction to the "String" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "String" in "Python"
# MAGIC * "<b>Strings</b>" are the "<b>Sequences</b>" of "<b>Unicode Code Points</b>". For the most part, the "<b>Unicode Code Points</b>" can be thought of as "<b>Characters</b>", but, these are "<b>Not Strictly Equivalent</b>".
# MAGIC * "<b>Strings</b>" in "<b>Python</b>" have the "<b>Data Type</b>" as "<b>str</b>", which is spelled as "<b>s-t-r</b>".
# MAGIC * The "<b>Sequence of Characters</b>" in a "<b>Python String</b>" is "<b>Immutable</b>", which means that "<b>Once a String is Constructed, its Contents Can't be Modified</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Literal String
# MAGIC * "<b>Literal Strings</b>" in "<b>Python</b>" are "<b>Delimited</b>" by "<b>Quotes</b>".
# MAGIC * Both of the "<b>Single Quotes</b>" and the "<b>Double Quotes</b>" can be used.
# MAGIC * However, the "<b>Type</b>" of the "<b>Quote</b>" used should be "<b>Consistent</b>". Example - it is "<b>Not Allowed</b>" to use "<b>Single Quote</b>" on "<b>One Side</b>", and, "<b>Double Quote</b>" on the "<b>Other Side</b>".
# MAGIC * "<b>Supporting</b>" both "<b>Quoting Style</b>" allows to "<b>Easily Incorporate</b>" the "<b>Other Quote Character</b>" into the "<b>String Literal</b>" without using the "<b>Escape Characters</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Using "Single Quote"
'My name is Oindrila Chakraborty'

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Using "Double Quote"
"My name is Oindrila Chakraborty"

# COMMAND ----------

# DBTITLE 1,"Literal String" Can't be Created Using "Single Quote" at "One End" and "Double Quote" at "Another End"
'My name is Oindrila Chakraborty"

# COMMAND ----------

# DBTITLE 1,Use "Single Quote" Inside a "Literal String" that is Created Using "Double Quote"
"My name is 'Oindrila Chakraborty'"

# COMMAND ----------

# DBTITLE 1,Use "Double Quote" Inside a "Literal String" that is Created Using "Single Quote"
'My name is "Oindrila Chakraborty"'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Literal String with "New Line"
# MAGIC * There are "<b>Two Options</b>" to "<b>Create</b>" a "<b>Literal Strings</b>" in "<b>Python</b>" with "<b>New Line</b>" in it.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Literal String with "New Line" Using "Multi-Line String"
# MAGIC * "<b>Multi-Line Strings</b>" in "<b>Python</b>" are "<b>Delimited</b>" by "<b>Three Quote Characters</b>", rather than "<b>One</b>".
# MAGIC * Both of the "<b>Single Quotes</b>" and the "<b>Double Quotes</b>" can be used.
# MAGIC * When the "<b>Literal String</b>" is "<b>Echoed Back</b>", it can be seen that the "<b>New Lines</b>" are "<b>Represented</b>" by the "<b>\n Escape Sequence</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" with "New Lines" Inside It, Using "Three Single Quote Characters"
'''I am 'Oindrila Chakraborty'.
I live at "Kankurgachi".
I am married.
'''

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" with "New Lines" Inside It, Using "Three Double Quote Characters"
"""I am 'Oindrila Chakraborty'.
I live at "Kankurgachi".
I am married.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Literal String with "New Line" Using "Escape Sequence"
# MAGIC * As an "<b>Alternative</b>" to using the "<b>Multi-Line Quoting</b>", it is possible to "<b>Embed</b>" the "<b>New Line Character</b>" in the "<b>Literal String</b>" itself.
# MAGIC * In "<b>Windows</b>", the "<b>New Line</b>" is "<b>Represented</b>" by the "<b>Carriage Return</b>" and the "<b>New Line Escape Character</b>coupled together" , i.e., "</b>\r\n</b>", but, there is "<b>No Need</b>" to do that in "<b>Python</b>".
# MAGIC * "<b>Python 3</b>" has a feature called "<b>Universal New Line Support</b>", which "<b>Translates</b>" from the simple "<b>\n</b>" to the "<b>Native New Line Sequence</b>" for the "<b>Platform</b>" on "<b>Input</b>" and "<b>Output</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" with "New Lines" Inside It, Using "\n Escape Sequence"
"I am 'Oindrila Chakraborty'.\nI live at 'Kankurgachi'.\nI am married."

# COMMAND ----------

# MAGIC %md
# MAGIC # Usage of Escape Sequence in Other Scenarios
# MAGIC * The "<b>Escape Sequences</b>" can be used for "<b>Other Purposes</b>" as well, such as the following -
# MAGIC * <b>1</b>. "<b>Tabs</b>" can be "<b>Incorporated</b>" within a "<b>Literal String</b>" by using the "<b>\t Escape Sequence</b>".
# MAGIC * <b>2</b>. "<b>Quote Characters</b>" can be "<b>Incorporated</b>" within a "<b>Literal String</b>" itself by using the "<b>\ " Escape Sequence</b>", or, "<b>\ ' Escape Sequence</b>".
# MAGIC <br>In this case, "<b>Python</b>" is smart enough to use the "<b>Most Convenient Quote Delimiter</b>".
# MAGIC <br>When using the "<b>\ " Escape Sequence</b>" within a "<b>Literal String</b>", "<b>Python</b>" will use the "<b>Single Quote</b>" to "<b>Delimit</b>" the "<b>Literal String</b>".
# MAGIC <br>When using the "<b>\ ' Escape Sequence</b>" within a "<b>Literal String</b>", "<b>Python</b>" will use the "<b>Double Quote</b>" to "<b>Delimit</b>" the "<b>Literal String</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Delimiting by "Single Quote" that Incorporates "Single Quote" Using "Escape Sequence"
'My Name is \'Oindrila\' and my Surname is \'Chakraborty\''

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Delimiting by "Double Quote" that Incorporates "Double Quote" Using "Escape Sequence"
"My Name is \"Oindrila\" and my Surname is \"Chakraborty\""

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Delimiting by "Single Quote" that Incorporates "Both Quotes" Using "Escape Sequence"
'My Name is \'Oindrila\' and my Surname is "Chakraborty"'

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" Delimiting by "Double Quote" that Incorporates "Both Quotes" Using "Escape Sequence"
"My Name is 'Oindrila' and my Surname is \"Chakraborty\""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Behaviour of "Escape Sequence" When "Both Quotes" are Used in a "Literal String" Using the "Escape Sequence"
# MAGIC * "<b>Python</b>" will also "<b>Resort</b>" to the "<b>Escape Sequences</b>" when "<b>Both</b>" types of "<b>Quotes</b>" are used in a "<b>Literal String</b>".
# MAGIC * Because "<b>Backslash</b>" has a special meaning, to "<b>Place</b>" a "<b>Backslash</b>" in a "<b>Literal String</b>", it needs to be "<b>Escaped</b>" with the "<b>Backslash</b>" itself.

# COMMAND ----------

# DBTITLE 1,Create a "Literal String" that Incorporates "Backslash" Itself Using "Escape Sequence"
"My Address is 221\2B M.G. Road"

# COMMAND ----------

# DBTITLE 1,Correct Way to Create a "Literal String" that Incorporates "Backslash" Itself Using "Escape Sequence"
"My Address is 221\\2B M.G. Road"

# COMMAND ----------

# MAGIC %md
# MAGIC * To "<b>Re-assure</b>" that there is "<b>Only One Backslash</b>" in the "<b>Literal String</b>", the ",b>print ()</b>" Function is used.

# COMMAND ----------

address = "My Address is 221\\2B M.G. Road"
print(address)

# COMMAND ----------

# MAGIC %md
# MAGIC # When the "Raw Strings" are Used?
# MAGIC * Sometimes, when particularly dealing with "<b>Literal Strings</b>", such as, "<b>Windows File System Paths</b>", or, "<b>Regular Expression Patterns</b>", which use the "<b>Backslashes</b>" extensively, the "<b>Requirement</b>" to "<b>Double Up</b>" on the "<b>Backslashes</b>" can be "<b>Ugly</b>" and "<b>Error-Prone</b>".
# MAGIC * In such cases, "<b>Python</b>" comes to the rescue with its "<b>Raw Strings</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is a "Raw String"?
# MAGIC * A "<b>Raw String</b>" "<b>Does Not Support</b>" any of the "<b>Escape Sequences</b>" and are very much "<b>What Is Seen</b>" is "<b>What It Is</b>".
# MAGIC * To "<b>Create</b>" a "<b>Raw String</b>", the "<b>Opening Quote</b>" is "<b>Prefixed</b>" with a "<b>Lowercase r</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Raw String" to "Display" a "Windows File Path" Using "Single Quote"
filePath = r'C:\Users\Oindrila\Documents\Python Programs'
print(filePath)

# COMMAND ----------

# DBTITLE 1,Create a "Raw String" to "Display" a "Windows File Path" Using "Double Quote"
filePath = r"C:\Users\Oindrila\Documents\Python Programs"
print(filePath)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create "Strings" Using "String Constructor"
# MAGIC * It is possible to "<b>Create</b>" the "<b>String Representations</b>" of "<b>Other Data Types</b>", such as, "<b>Integers</b>", "<b>Floats</b>", "<b>Booleans</b>" etc.

# COMMAND ----------

# DBTITLE 1,Create a "String Representations" of an "Integer" Using "String Constructor"
str(88)

# COMMAND ----------

# DBTITLE 1,Create a "String Representations" of a "Float" Using "String Constructor"
str(88.56094367)

# COMMAND ----------

# DBTITLE 1,Create a "String Representations" of a "Boolean" Using "String Constructor"
str(True)
