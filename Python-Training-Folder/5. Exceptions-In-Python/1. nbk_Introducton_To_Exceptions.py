# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Exceptions"
# MAGIC * Topic: Introduction to the "Exceptions" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "Exception Handling" is "Required"?
# MAGIC * In most programs, there is a "<b>Clear Notion</b>" of the "<b>Normal Path</b>" through the "<b>Code</b>".
# MAGIC * But, "<b>Conditions</b>" can "<b>Arise</b>" where the "<b>Normal Path</b>" "<b>Can't</b>" be "<b>Followed</b>".
# MAGIC * Example: if a program involves "<b>Reading a File</b>", "<b>Specified</b>" by the "<b>User</b>", it may actually happen that the "<b>File</b>" "<b>Doesn't Exist</b>. "<b>Conditions</b>" like this often "<b>Needs to be Handled</b>". Such "<b>Conditions</b>" are "<b>Called</b>" as "<b>Exceptions</b>".
# MAGIC * The "<b>Standard Mechanism</b>" for "<b>Handling</b>" such "<b>Conditions</b>" in "<b>Python</b>", as with many other "<b>Programming Languages</b>", is what are known as "<b>Exception Handling</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Exception Handling"?
# MAGIC * "<b>Exception Handling</b>" is a "<b>Mechanism</b>" for "<b>Stopping</b>" the "<b>Normal Flow</b>" of a "<b>Program</b>", and, "<b>Continuing</b>" at some "<b>Surrounding Context</b>" ,or, "<b>Code Block</b>" in the "<b>Same Program</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Raising an Exception" - What Does It Mean?
# MAGIC * The "<b>Event</b>" of "<b>Interrupting</b>" the "<b>Normal Flow</b>" of a "<b>Program</b>" is "<b>Called</b>" the "<b>Act</b>" of "<b>Raising an Exception</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Handling an Exception" - What Does It Mean?
# MAGIC * In some "<b>Enclosing Context</b>", the "<b>Raised Exception</b>" must be "<b>Handled</b>", upon which the "<b>Control Flow</b>" of the "<b>Program</b>" is "<b>Transferred</b>" to the "<b>Exception Handler</b>".

# COMMAND ----------

# MAGIC %md 
# MAGIC # "Unhandled Exceptions" - What Does It Mean?
# MAGIC * If an "<b>Exception</b>" "<b>Propagates Up</b>" the "<b>Callstack</b>" to the "<b>Start</b>" of a "<b>Program</b>", then an "<b>Unhandled Exception</b>" will cause the "<b>Program</b>" to "<b>Terminate</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is an "Exception Object"?
# MAGIC * An "<b>Exception Object</b>" containing the "<b>Information</b>" about "<b>Where</b>" and "<b>Why</b>" an "<b>Exception Event Occurred</b>" is "<b>Transported</b>" from the point, at which the "<b>Exception</b>" was "<b>Raised</b>" to the "<b>Exception Handler</b>", so that the "<b>Handler</b>" can "<b>Interrogate</b>" the "<b>Exception Object</b>", and, "<b>Take</b>" the "<b>Appropriate Action</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Call" a "Function" That Will "Raise" an "Exception" Without "Exception Handler"

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

# DBTITLE 1,Create a "Function" That "Attempts" to "Construct" an "Integer" From "String"
def convertFromStringToInt(strInt):
  numInStr = ''
  for eachStrInt in strInt:
    numInStr += wordToDigitDict[eachStrInt]
  numInInt = int(numInStr)
  return numInInt

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With a "Sequence of Strings", "Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("two nine eight eight".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# MAGIC %md
# MAGIC * When the "<b>Function</b>", i.e., "<b>convertFromStringToInt ()</b>" is "<b>Called</b>" with "<b>Strings</b>" that are "<b>Not Present</b>" in the "<b>Dictionary</b>", i.e., "<b>wordToDigitDict</b>", a "<b>Traceback</b>" from the "<b>Dictionary Lookup</b>" is "<b>Displayed</b>".
# MAGIC * The "<b>Dictionary</b>", i.e., "<b>wordToDigitDict</b>" here "<b>Raised</b>" a "<b>KeyError</b>" when the "<b>First String</b>" sent to the "<b>Function</b>", i.e., "<b>convertFromStringToInt ()</b>", which is "<b>my</b>", is "<b>Looked Up</b>" in the "<b>Dictionary</b>", because, there is "<b>No Item</b>" in the "<b>Dictionary</b>" with the "<b>Key</b>" as "<b>my</b>".
# MAGIC * Since, there was "<b>No Exception Handler</b>" in place, the "<b>Exception</b>" was "<b>Caught</b>", and, the "<b>Stack Trace</b>" was "<b>Displayed</b>".
# MAGIC * The String "<b>KeyError</b>" in the "<b>Stack Trace</b>" refers to the "<b>Type</b>" of the "<b>Exception Object</b>".
# MAGIC * The String "<b>my</b>" is the "<b>Error Message</b>", which is the part of the "<b>Payload</b>" of the "<b>Exception Object</b>" that has been "<b>Retrieved</b>" and "<b>Displayed</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Call" a "Function" That Will "Raise" an "Exception" With "Exception Handler"

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Handle" the "Exceptions" Using "try-except Block"
# MAGIC * Both the "<b>try</b>" and "<b>except</b>" Keywords "<b>Introduce</b>" "<b>New Blocks</b>".
# MAGIC * The "<b>try</b>" <b>Block</b> contains the "<b>Code</b>" that could "<b>Raise</b>" an "<b>Exception</b>".
# MAGIC * The "<b>except</b>" <b>Block</b> contains the "<b>Code</b>", which "<b>Performs</b>" the "<b>Error Handling</b>" in the "<b>Event</b>" that an "<b>Exception</b>" is "<b>Raised</b>".

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError" Exception
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
  except KeyError:
    # If an "Unconvertible String" is "Supplied", "-1" should be "Returned".
    print("Conversion Failed!!")
    numInInt = -1
  return numInInt

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With a "Sequence of Strings", "Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("two nine eight eight".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# MAGIC %md
# MAGIC * When the Function "<b>convertFromStringToInt ()</b>" is "<b>Modified</b>" with the "<b>try-except Block</b>", the "<b>print () Statement</b>" in the "<b>try</b>" <b>Block</b>, after the point at which the "<b>Exception</b>" was "<b>Raised</b>" was "<b>Not executed</b>", when "<b>Sequence of Strings</b>" were "<b>Passed</b>" to the Function that "<b>Didn't Exist</b>" in the "<b>Dictionary</b>".
# MAGIC * Instead, the "<b>Execution</b>" was "<b>Transferred Directly</b>" to the "<b>First Statement</b>" of the "<b>except</b>" <b>Block</b>.

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC * The Function "<b>convertFromStringToInt ()</b>" expects its "<b>Argumemt</b>" to be "<b>Iterable</b>".
# MAGIC * When an "<b>Argumemt</b>" is "<b>Passed</b>" to the Function "<b>convertFromStringToInt ()</b>" that is "<b>Non-Iterable</b>", like an "<b>Integer</b>", the "<b>Exception Handler</b>", present in the Function "<b>convertFromStringToInt ()</b>" "<b>Could Not Intercept</b>" the "<b>Exception</b>".
# MAGIC * This time, a "<b>TypeError</b>" <b>Exception</b> is "<b>Raised</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Handle" the "Exceptions" Using Multiple "except Blocks" With Each "try Block"
# MAGIC * Each "<b>try</b>" <b>Block</b> can have "<b>Multiple</b>" corresponding "<b>except</b>" <b>Blocks</b>, which can "<b>Intercept</b>" the "<b>Exceptions</b>" of "<b>Different Types</b>".

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions
def convertFromStringToInt(strInt):
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
  except KeyError:
    # If an "Unconvertible String" is "Supplied", "-1" should be "Returned".
    print("Conversion Failed!!")
    numInInt = -1
  except TypeError:
    # If an "Iterable" is "Not Supplied", "-1" should be "Returned".
    print("Conversion Failed!!")
    numInInt = -1
  return numInInt

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Handle" the "Exceptions" By "Merging" the Multiple "except Blocks" With Each "try Block"
# MAGIC * There are some "<b>Code Duplication</b>" between the "<b>Two Exception Handlers</b>", i.e., "<b>KeyError</b>", and, "<b>TypeError</b>" with the "<b>Duplicated print () Statement</b>", and, "<b>Duplicated Assignment</b>".
# MAGIC * Since, "<b>Both</b>" of the "<b>Exception Handlers</b>", i.e., "<b>KeyError</b>", and, "<b>TypeError</b>" do the "<b>Same Thing</b>", it is possible to "<b>Collapse</b>" the "<b>Multiple Exception Handlers</b>" into "<b>One Exception Handler</b>" by "<b>Using</b>" the "<b>Ability</b>" of the "<b>except</b>" <b>Statement</b> to "<b>Accept</b>" a "<b>Tuple</b>" of "<b>Exception</b>" Types.

# COMMAND ----------

# DBTITLE 1,Modify the Function That "Attempts" to "Construct" an "Integer" From "String" With "try-except Block"
# Modify the Function "convertFromStringToInt ()" to "Handle" the "KeyError", and, "TypeError" Exceptions by "Merging" the Multiple "except Blocks" 
def convertFromStringToInt(strInt):
  numInInt = -1
  try:
    numInStr = ''
    for eachStrInt in strInt:
      numInStr += wordToDigitDict[eachStrInt]
    numInInt = int(numInStr)
    print(f"Conversion Succeded!! number = {numInInt}")
  except (KeyError, TypeError):
    # If an "Unconvertible String" is "Supplied", or, an "Iterable" is "Not Supplied", "-1" should be "Returned".
    print("Conversion Failed!!")
  return numInInt

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Sequence of Strings" That are "Not Present" in the Dictionary "wordToDigitDict"
convertFromStringToInt("my name is oindrila chakraborty".split())

# COMMAND ----------

# DBTITLE 1,Call the Function "convertFromStringToInt ()" With "Non-Iterable" Data Type, e.g. Integer
convertFromStringToInt(2988)
