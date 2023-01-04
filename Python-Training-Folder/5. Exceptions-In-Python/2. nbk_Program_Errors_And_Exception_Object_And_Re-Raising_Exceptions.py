# Databricks notebook source
# MAGIC %md
# MAGIC # "Program Errors", "Exception Objects", and, "Re-Raising Exceptions"
# MAGIC * Topic: "Program Errors", "Exception Objects", and, "Re-Raising Exceptions" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to "Program Errors"
# MAGIC * Almost anything that "<b>Goes Wrong</b>" with a "<b>Python Program</b>" "<b>Results</b>" in an "<b>Exception</b>".
# MAGIC * But, some, such as "<b>IndentationError</b>", "<b>SyntaxError</b>", and, "<b>NameError</b>" are the "<b>Result</b>" of the "<b>Programmer's Error</b>", which should be "<b>Identified</b>", and, "<b>Corrected</b>" during "<b>Development</b>", rather than "<b>Handled At Runtime</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * If the "<b>print ()</b>" <b>Statement</b> is "<b>Removed</b>" from the "<b>except</b>" <b>Block</b> from the Function "<b>convertFromStringToInt ()</b>", and, the "<b>Definition</b>" of the Function is "<b>Executed</b>", an "<b>IndentationError</b>" <b>Exception</b> is "<b>Raised</b>", because, the "<b>except</b>" <b>Block</b> is now "<b>Empty</b>", and, "<b>Empty Blocks</b>" are "<b>Not Permitted</b>" in "<b>Python</b>".
# MAGIC * The "<b>IndentationError</b>" <b>Exception</b> is "<b>Not</b>" an "<b>Exception</b>" that is ever useful to "<b>Catch</b>" with an "<b>except</b>" <b>Block</b>.

# COMMAND ----------

# DBTITLE 1,Create a "Dictionary", Where the "Keys" are in "Words", and, the Corresponding "Values" are in "Digits"
# Both the "Keys" and the "Values" are of "String" Data Type
wordToDigitDict = {
  "zero": '0',
  "one": '1',
  "two": '2',
  "three": '3',
  "four": '4',
  "five": '5',
  "six": '6',
  "seven": '7',
  "eight": '8',
  "nine": '9',
}

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by "Merging" the Multiple "except Blocks", but, "Remove" the "print ()" Statement from the "except" Block
def convertFromStringToInt(strInt):
  numInInt = -1
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
  except (KeyError, TypeError):
  return numInInt

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "No-Op" in "except" Block
# MAGIC * The "<b>pass</b>" <b>Keyword</b> is a "<b>Special Statememt</b>" that "<b>Does Precisely Nothing</b>". It is a "<b>no-op</b>".
# MAGIC * "<b>Only Purpose</b>" of the "<b>pass</b>" <b>Keyword</b> is to "<b>Allow</b>" the "<b>Developers</b>" to "<b>Construct</b>" the "<b>Syntactically Permissible Blocks</b>" that are "<b>Semantically Empty</b>".

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by using the "pass" Keyword in the "except" Block
def convertFromStringToInt(strInt):
  numInInt = -1
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
  except (KeyError, TypeError):
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", then, "Do Nothing".
    pass
  return numInInt

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Handle" the "Exceptions" By "Returning" "Multiple Return Statements" From the "try-except Block"
# MAGIC * It is possible to "<b>Use</b>" "<b>Multiple Return Statements</b>" in "<b>Both</b>" of the "<b>try</b>" and "<b>except</b>" <b>Blocks</b>.

# COMMAND ----------

# MAGIC %md
# MAGIC * It would be "<b>Better</b>" to "<b>Simplify Further</b>" and just use "<b>Multiple Return Statements</b>", and, "<b>Do Away</b>" with the "<b>numInInt</b>" <b>Variable Completely</b> in the Function "<b>convertFromStringToInt ()</b>".

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by using "Multiple Return Statements" in "Both" of the "try" and "except" Blocks
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    return int(numInStr)
  except (KeyError, TypeError):
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", "-1" should be "Returned".
    return -1

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Accessing" the "Exception Object"
# MAGIC * Sometimes, it is required to "<b>Interrogate</b>" the "<b>Exception Objects</b>", like the "<b>KeyError</b>", or, "<b>TypeError</b>" for "<b>More Details</b>" of "<b>What Went Wrong</b>".
# MAGIC * It is possible to get a "<b>Named Reference</b>" to the "<b>Exception Object</b>" by "<b>Tagging</b>" an "<b>as</b>" <b>Clause</b> onto the "<b>End</b>" of the "<b>except</b>" <b>Statement</b>.

# COMMAND ----------

# DBTITLE 1,Display the "Error Message", "Caught" in the "except" Block
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by "Displaying" the "Error Message" in the "except" Block
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    return int(numInStr)
  except (KeyError, TypeError) as e:
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", then, "Print" the "Error Message".
    print(f"Conversion Error : {e}")

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Print</b>" a "<b>Message</b>" with the "<b>Exception Details</b>" to the "<b>Standard Error Stream</b>" "<b>Before Returning</b>" from the Function "<b>convertFromStringToInt ()</b>" "<b>Call</b>".
# MAGIC * To "<b>Print</b>" a "<b>Standard Error</b>", a "<b>Reference</b>" to the "<b>Standard Error Stream</b>" is required from the "<b>sys</b>" <b>Module</b>. So, "<b>At the Top</b>" of the "<b>Program</b>", the "<b>sys</b>" <b>Module</b> needs to be "<b>Imported</b>".
# MAGIC * Afterwards, it is possible to "<b>Pass</b>" the "<b>sys.stderr</b>" as a "<b>Keyword Argument</b>", called "<b>file</b>" to "<b>Print</b>".
# MAGIC * In the case of "<b>Exceptions</b>", using an "<b>!r</b>" in the "<b>F-String</b>" approach, gives "<b>More Detailed Information</b>" about the "<b>Type</b>" of the "<b>Exception</b>".

# COMMAND ----------

# DBTITLE 1,Display the "Error Message" to the "Standard Error Stream", "Caught" in the "except" Block
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by "Displaying" the "Error Message" to the "Standard Error Stream" in the "except" Block
import sys

def convertFromStringToInt(strInt):
  numInInt = -1
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
    return numInInt
  except (KeyError, TypeError) as e:
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", then, "Print" the "Error Message".
    print(f"Conversion Error : {e!r}", file = sys.stderr)

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Re-Raising Exceptions"
# MAGIC * "<b>Without</b>" a "<b>Parameter</b>", the "<b>raise</b>" <b>Statement</b> simply "<b>Re-Raises</b>" the "<b>Exception</b>" that is "<b>Currently Handled</b>" to the "<b>Calling Context</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * It is possible to "<b>Call</b>" the Function "<b>convertFromStringToInt ()</b>" from "<b>Another Function</b>", which "<b>Calls</b>" the Function "<b>convertFromStringToInt ()</b>", and, "<b>Computes</b>" the "<b>Natural Log</b>" of the "<b>Result</b>".

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by using "Multiple Return Statements" in "Both" of the "try" and "except" Blocks
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    return int(numInStr)
  except (KeyError, TypeError):
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", "-1" should be "Returned".
    return -1

# COMMAND ----------

# DBTITLE 1,Create a "Function" That "Computes" the "Natural Log" of the "Number" Passed as an "Argument"
from math import log

def getNaturalLog (strNum):
  number = convertFromStringToInt(strNum)
  return log(number)

# COMMAND ----------

# DBTITLE 1,Call the Function "getNaturalLog ()" With a "Sequence of Strings", "Present" in the Dictionary "wordToDigitDict"
getNaturalLog("two nine eight eight".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "getNaturalLog ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
getNaturalLog("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "getNaturalLog ()" With "Non-Iterable" Data Type, e.g. Integer
getNaturalLog(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC * A "<b>Slightly Better Program</b>" might "<b>Test</b>" the "<b>Value</b>" of the <b>Variable</b> "<b>number</b>", "<b>Before Proceeding</b>" to the "<b>Call</b>" to the <b>Python Built-In Function</b>, i.e., "<b>log ()</b>".
# MAGIC * "<b>Without</b>" such a "<b>Check</b>", the "<b>Call</b>" to the <b>Python Built-In Function</b>, i.e., "<b>log ()</b>" will "<b>Fail</b>", when the "<b>Arguments</b>" are "<b>Passed</b>" to the Function "<b>getNaturalLog ()</b>", which are either "<b>Unconvertible String</b>", or, "<b>Not Iterable</b>", as <b>displayed above</b>.
# MAGIC * Naturally, the "<b>Failure</b>" in "<b>Calling</b>" the <b>Python Built-In Function</b>, i.e., "<b>log ()</b>" causes the "<b>Raising</b>" of "<b>Another Exception</b>", i.e., "<b>ValueError</b>" <b>Exception</b>, which "<b>Hides</b>" the "<b>Original Exception Type</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * It is "<b>Much Better</b>", and, "<b>Altogether</b>" more "<b>Pythonic</b>" to "<b>Raise</b>" an "<b>Exception</b>" from the Function "<b>convertFromStringToInt ()</b>".
# MAGIC * Instead of "<b>Returning</b>" an "<b>Un-Pythonic Error Code</b>", i.e., "<b>-1</b>", it is possible to "<b>Re-Raise</b>" the "<b>Exception Object</b>" that is "<b>Currently Handled</b>" from the Function "<b>convertFromStringToInt ()</b>".
# MAGIC * This can be done by "<b>Replacing</b>" the "<b>return -1</b>" <b>Statement</b> with "<b>raise</b>" <b>Statement</b> "<b>At the End</b>" of the "<b>Exception Handling</b>" <b>Block</b>.

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by "Re-Raising" the "Exception" to the "Calling Context" in the "except" Block
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    return int(numInStr)
  except (KeyError, TypeError) as e:
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", "Re-Raise" the "Exception".
    raise

# COMMAND ----------

# DBTITLE 1,Call the Function "getNaturalLog ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
getNaturalLog("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "getNaturalLog ()" With "Non-Iterable" Data Type, e.g. Integer
getNaturalLog(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC * It can be seen that the "<b>Original Exception Type</b>" is "<b>Re-Raised</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Standard Exception Types" in "Python"
# MAGIC * "<b>Python Developers</b>" should be careful to "<b>Avoid</b>" the "<b>Mistake</b>" of having "<b>Too Tight Scopes</b>" for "<b>Exception Handling</b>" <b>Blocks</b>.
# MAGIC * It is possible to use <b>One</b> "<b>try-except</b>" <b>Block</b> for "<b>All</b>" of the "<b>Calls</b>" to a "<b>Program</b>".
# MAGIC * "<b>Python</b>" provides several "<b>Standard Exception Types</b>" to "<b>Signal</b>" the "<b>Common Errors</b>".
# MAGIC * Example - if a "<b>Function Parameter</b>" is "<b>Supplied</b>" with "<b>Illegal Value</b>", i.e., the "<b>Data Type</b>" of the "<b>Function Parameter</b>" is "<b>Right</b>", but, the "<b>Value</b>" is "<b>Inappropriate</b>", it is "<b>Customary</b>" to "<b>Raise</b>" a "<b>ValueError</b>".
# MAGIC * The "<b>ValueError</b>" <b>Exception</b> can be "<b>Raised</b>" by using the "<b>raise</b>" <b>Keyword</b> with a "<b>Newly Created Exception Object</b>", which can be "<b>Created</b>" by "<b>Calling</b>" the "<b>ValueError ()</b>" <b>Constructor</b>.

# COMMAND ----------

# DBTITLE 1,Create a Function to "Divide" "One Number" by "Another Number"
# "Create" a Function "divideNumbers" to "Divide" "One Number" by "Another Number", and, "Handle" the "Exceptions" by "Raising" the "Exception" from inside the "Exception Handler", i.e., "except" Block
def divideNumbers(numerator, denominator):
  try:
    return numerator/denominator
  except ZeroDivisionError:
    raise ValueError("Value Error Occurred")

# COMMAND ----------

# DBTITLE 1,Call the Function "divideNumbers ()" With Parameters (12, 0)
divideNumbers(12, 0)

# COMMAND ----------

# DBTITLE 1,Create a Function to "Divide" "One Number" by "Another Number"
# "Create" a Function "divideNumbers" to "Divide" "One Number" by "Another Number", and, "Handle" the "Exceptions" by "Raising" the "Exception" "Without" using the "try-except" Block
def divideNumbers(numerator, denominator):
  if denominator > 0:
    return numerator/denominator
  else:
    raise ValueError("Value Error Occurred")

# COMMAND ----------

# DBTITLE 1,Call the Function "divideNumbers ()" With Parameters (12, 0)
divideNumbers(12, 0)
