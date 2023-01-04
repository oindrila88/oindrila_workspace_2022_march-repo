# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Generators"
# MAGIC * Topic: Introduction to "Generators" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Generator"?
# MAGIC * "<b>Python Generators</b>" "<b>Provide</b>" the "<b>Means</b>" for "<b>Describing</b>" the "<b>Iterable Series</b>" with "<b>Code</b>" in "<b>Functions</b>".
# MAGIC * These "<b>Sequences</b>" are "<b>Evaluated</b>" "<b>Lazily</b>", meaning that, the "<b>Python Generators</b>" "<b>Only Compute</b>" the "<b>Next Value</b>" "<b>On Demand</b>".
# MAGIC * This "<b>Important Property</b>" "<b>Allows</b>" the "<b>Python Generators</b>" to "<b>Model</b>" the "<b>Infinite Sequences of Values</b>" with "<b>No Definite End</b>", such as - "<b>Streams of Data from a Sensor</b>", or, "<b>Active Log Files</b>".
# MAGIC * By "<b>Carefully Designing</b>" the "<b>Generator Function</b>", it is possible to make "<b>Generic Stream Processing Elements</b>", which can be "<b>Composed</b>" into "<b>Sophisticated Pipelines</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to "Create" the "Generator Functions"?
# MAGIC * "<b>Python Generators</b>" are "<b>Defined</b>" by "<b>Any Python Function</b>", which "<b>Uses</b>" the "<b>yield</b>" <b>Keyword</b> "<b>At Least Once</b>" in its "<b>Definition</b>".
# MAGIC * The "<b>Python Generators</b>" may also contain the "<b>return</b>" <b>Keyword</b> with "<b>No Arguments</b>".
# MAGIC * Just like "<b>Any Other Function</b>", there is an "<b>Implicit</b>" "<b>return</b>" at the "<b>End</b>" of the "<b>Definition</b>".
# MAGIC * Following is the "<b>General Form</b>" of "<b>Creating</b>" a "<b>Generator Function</b>" in "<b>Python</b>" -
# MAGIC   * "<b>Generator Functions</b>" are "<b>Introduced</b>" by "<b>def</b>" <b>Keyword</b>, just like a "<b>Regular Python Function</b>".
# MAGIC   * Then "<b>yield</b>" the "<b>Values</b>".
# MAGIC * A "<b>Generator Function</b>" can be "<b>Called</b>" just like a "<b>Regular Python Function</b>", and, its "<b>Return Value</b>" can be "<b>Assigned</b>" to a "<b>Variable</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Generator Function" That "Returns" "Five Strings" of "Names"
def genFiveNames ():
  yield "Oindrila"
  yield "Soumyajyoti"
  yield "Premanshu"
  yield "Rama"
  yield "Kasturi"

# COMMAND ----------

# DBTITLE 1,"Call" the "Generator Function" and "Assign" to a "Variable"
fiveNames = genFiveNames ()
print(fiveNames)
print(type(fiveNames))

# COMMAND ----------

# MAGIC %md
# MAGIC # What Does a "Generator Function" Returns?
# MAGIC * A "<b>Generator Function</b>" "<b>Returns</b>" a "<b>Generator Object</b>".
# MAGIC * "<b>Generators</b>" are in fact "<b>Python Iterators</b>".
# MAGIC * So, the "<b>Iterator Protocols</b>" can be used to "<b>Retrieve</b>", or, "<b>Yield</b>" the "<b>Successive Values</b>" from the "<b>Series</b>".

# COMMAND ----------

# DBTITLE 1,"Retrieve" the "Successive Values" from the "Generator Object", i.e., "fiveNames" Using the Function "next ()"
print(next(fiveNames))
print(next(fiveNames))
print(next(fiveNames))
print(next(fiveNames))
print(next(fiveNames))

# COMMAND ----------

# MAGIC %md
# MAGIC # When the "Generator Object" Reaches the "End"
# MAGIC * When the "<b>Last Value</b>" from the "<b>Generator Object</b>" has been "<b>Yielded</b>", "<b>Subsequent Calls</b>" to the <b>Built-In Function</b> "<b>next ()</b>" "<b>Raises</b>" a "<b>StopIteration</b>" "<b>Exception</b>", just like any "<b>Other Python Iterator</b>".

# COMMAND ----------

# DBTITLE 1,What Happens When "Generator Object" Reaches the "End"?
print(next(fiveNames))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Generators" in "Other Interable Constructs"
# MAGIC * Since, the "<b>Generator Objects</b>" are "<b>Iterator Objects</b>", and, the "<b>Iterator Objects</b>" must be "<b>Iterable</b>", the "<b>Generator Objects</b>" can also be used in "<b>All</b>" the usual "<b>Python Constructs</b>", which "<b>Expect</b>" "<b>Iterable Objects</b>", such as - "<b>for</b>" <b>Loop</b>.

# COMMAND ----------

# DBTITLE 1,"Iterate Over" a "Generator Object", "Retrieved From" a "Generator Function" Using "for" Loop
for name in genFiveNames ():
  print(name)

# COMMAND ----------

# MAGIC %md
# MAGIC # "Generators" are "Single-Use Objects"
# MAGIC * One "<b>Important</b>" thing to "<b>Keep in Mind</b>" is that "<b>Each Call</b>" to the "<b>Generator Function</b>" "<b>Returns</b>" a "<b>New Generator Object</b>".
# MAGIC * "<b>Each</b>" of the "<b>Generator Objects</b>" can be "<b>Advanced</b>" "<b>Independently</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Call</b>" the "<b>Generator Function</b>", i.e., "<b>genFiveNames ()</b>" "<b>Two Times</b>", "<b>Binding</b>" the "<b>Results</b>" to the "<b>Two Variables</b>", i.e., "<b>fiveNames1</b>", and, "<b>fiveNames2</b>" respectively.
# MAGIC * It can be seen that, the "<b>Two Variables</b>", i.e., "<b>fiveNames1</b>", and, "<b>fiveNames2</b>" are "<b>Two Different Generator Objects</b>".

# COMMAND ----------

# DBTITLE 1,"Call" the "Generator Function", i.e., "genFiveNames ()" "Two Times"
fiveNames1 = genFiveNames ()
fiveNames2 = genFiveNames ()

print(fiveNames1)
print(fiveNames2)

print(fiveNames1 is fiveNames2)

# COMMAND ----------

# MAGIC %md
# MAGIC * "<b>Each</b>" of the "<b>Generator Objects</b>", i.e., "<b>fiveNames1</b>", and, "<b>fiveNames2</b>", can be "<b>Advanced</b>" "<b>Independently</b>".

# COMMAND ----------

print(next(fiveNames1))
print(next(fiveNames2))
print(next(fiveNames1))
print(next(fiveNames2))

# COMMAND ----------

# MAGIC %md
# MAGIC # "Maintaining State" in "Generators"
# MAGIC * The "<b>Generator Function</b>" "<b>Resumes Execution</b>" "<b>Each Time</b>" when the "<b>Next Value</b>" is "<b>Requested</b>", and, that "<b>State</b>" can be "<b>Maintained</b>" in the "<b>Local Variables</b>".
# MAGIC * The "<b>Resumable Nature</b>" of the "<b>Generator Functions</b>" can "<b>Result</b>" in "<b>Complex Control Flow</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC * In the following example, "<b>Two Generator Functions</b>" will be "<b>Created</b>", which "<b>Demonstrate</b>" the "<b>Lazy Evaluation</b>", and, then the "<b>Two Generator Functions</b>" will be "<b>Combined</b>" into a "<b>Generator Pipeline</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Create" the "First Generator Function"
# MAGIC * The "<b>First Generator Function</b>", i.e., "<b>takeElements ()</b>" will "<b>Retrieve</b>" a "<b>Specified Number of Elements</b>" from the "<b>Front</b>" of an "<b>Iterable</b>".
# MAGIC * The "<b>Python Function</b>", i.e., "<b>takeElements ()</b>" is considered as a "<b>Generator Function</b>" because, it contains "<b>At Least One</b>" "<b>yield</b>" <b>Statement</b>.
# MAGIC * The "<b>Generator Function</b>", i.e., "<b>takeElements ()</b>" also contains a "<b>return</b>" <b>Statement</b> to "<b>Terminate</b>" the "<b>Stream</b>" of the "<b>Yielded Values</b>".
# MAGIC * The "<b>Generator Function</b>", i.e., "<b>takeElements ()</b>" simply uses a "<b>Counter Variable</b>", i.e., "<b>counter</b>" to "<b>Keep Track</b>" of "<b>How Many Elements</b>" have been "<b>Yielded So Far</b>", and, the "<b>Sequence</b>" is "<b>Ended</b>" when the "<b>Specified Point</b>" is "<b>Reached</b>".

# COMMAND ----------

# DBTITLE 1,"Create" the "First Generator", i.e., "takeElements ()" to "Retrieve" a "Specified Number of Elements" from the "Front" of an "Iterable"
def takeElements (count, iterable):
  counter = 0
  for item in iterable:
    if counter == count:
      return
    counter += 1
    yield item

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Create" the "Second Generator Function"
# MAGIC * The "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" will "<b>Eliminate</b>" the "<b>Duplicate Elements</b>" by "<b>Keeping Track</b>" of "<b>Which Element</b>" has the "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" "<b>Already Seen</b>" in the "<b>Resultant Set</b>".
# MAGIC * The "<b>continue</b>" <b>Statement</b> "<b>Finishes</b>" the "<b>Current Iteration</b>" of the "<b>Loop</b>", and, "<b>Begins</b>" the "<b>Next Iteration</b>" "<b>Immediately</b>".
# MAGIC * When the "<b>continue</b>" <b>Statement</b> in the "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" is "<b>Executed</b>", the "<b>Execution</b>" will be "<b>Transferred Back</b>" to the "<b>for</b>" <b>Statement</b>, and, the "<b>continue</b>" <b>Statement</b> will "<b>Skip</b>" "<b>Any Value</b>", which have "<b>Already been Yielded</b>"

# COMMAND ----------

# DBTITLE 1,"Create" the "Second Generator", i.e., "eliminateDuplicateElements ()" to "Eliminate" the "Duplicate Elements" from the "Front" of an "Iterable"
def eliminateDuplicateElements (iterable):
  setOfUniqueElements = set()
  for item in iterable:
    if item in setOfUniqueElements:
      continue
    yield item
    setOfUniqueElements.add(item)

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Create" the "Generator Pipeline"
# MAGIC * "<b>Both</b>" of the "<b>Generator Functions</b>" are "<b>Arranged</b>" into a "<b>Lazy Pipeline</b>" using the "<b>First Generator Function</b>", i.e., "<b>takeElements ()</b>", and, the "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" "<b>Together</b>" to "<b>Fetch</b>" the "<b>First Three Unique Elements</b>" from a "<b>Collection</b>", i.e., "<b>listOfIntNumbers</b>".
# MAGIC * The "<b>Result</b>" of the "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" is "<b>Passed</b>" to the "<b>First Generator Function</b>", i.e., "<b>takeElements ()</b>", and, the "<b>Result</b>" is "<b>Looped Over</b>".
# MAGIC * The "<b>Second Generator Function</b>", i.e., "<b>eliminateDuplicateElements ()</b>" only does just enough work to "<b>Satisfy</b>" the "<b>Demands</b>" of the "<b>First Generator Function</b>", i.e., "<b>takeElements ()</b>", which it is "<b>Iterating Over</b>".
# MAGIC * The "<b>First Generator Function</b>", i.e., "<b>takeElements ()</b>" "<b>Never Gets</b>" "<b>As Far As</b>" the "<b>Last Two Elements</b>" in the "<b>Source List</b>", because, those are "<b>Not Needed</b>" to "<b>Produce</b>" the "<b>First Three Unique Elements</b>".

# COMMAND ----------

# DBTITLE 1,"Create" the "Lazy Pipeline" Function, i.e., "runPipeline ()"
def runPipeline ():
  listOfIntNumbers = [3, 6, 6, 2, 1, 1]
  for number in takeElements (3, eliminateDuplicateElements (listOfIntNumbers)):
    print(number)

# COMMAND ----------

# DBTITLE 1,"Call" the "Lazy Pipeline" Function, i.e., "runPipeline ()"
runPipeline ()

# COMMAND ----------

# MAGIC %md
# MAGIC # "Lazy Evaluation" of the "Python Generator Functions"
# MAGIC * "<b>Python Generator Functions</b>" are "<b>Lazy</b>", meaning that the "<b>Computation</b>" "<b>Only Happens</b>" "<b>Just-In-Time</b>", when the "<b>Next Result</b>" is "<b>Requested</b>".
# MAGIC * This "<b>Interesting</b>", and, "<b>Useful</b>" "<b>Property</b>" of "<b>Generators</b>" means that the "<b>Generators</b>" can be "<b>Used</b>" to "<b>Create</b>" the "<b>Infinite Sequences</b>".
# MAGIC * Since, the "<b>Values</b>" are "<b>Only Produced</b>" as "<b>Requested</b>" by the "<b>Caller</b>", and, "<b>No Data Structure</b>" needs to be "<b>Built</b>" to "<b>Contain</b>" the "<b>Elements</b>" of the "<b>Sequence</b>", the "<b>Generators</b>" can "<b>Safely</b>" be "<b>Used</b>" to "<b>Produce</b>" the "<b>Never-Ending</b>", or, just, "<b>Very Large Sequences</b>", like the following -
# MAGIC   * <b>Sensor Readings</b>
# MAGIC   * <b>Mathemetical Sequences</b>
# MAGIC   * <b>Contents of Multi-Terabyte Files</b>

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Create" an "Infinite Python Generator Function"
# MAGIC * In the following example of the "<b>Infinite Generator Function</b>", the "<b>Lucas Series</b>" is "<b>Implemented</b>", where the "<b>Series</b>" "<b>Starts With</b>" the "<b>Values</b>" "<b>2</b>", and, "<b>1</b>".
# MAGIC * "<b>Each Value</b>" "<b>After</b>" that is the "<b>Sum</b>" of the "<b>Two Preceeding Values</b>".
# MAGIC * So, the "<b>First Few Values</b>" of the "<b>Lucas Series</b>" are - "<b>2</b>", "<b>1</b>", "<b>3</b>", "<b>4</b>", "<b>7</b>", and, "<b>11</b>".
# MAGIC * The "<b>First</b>" "<b>yield</b>" <b>Statement</b>, inside the "<b>Generator Function</b>", i.e., "<b>generateLucasSeries ()</b>", "<b>Produces</b>" the "<b>Value</b>" "<b>2</b>".
# MAGIC * Then the "<b>Generator Function</b>", i.e., "<b>generateLucasSeries ()</b>" "<b>Initializes</b>" the "<b>Two Variables</b>", i.e., "<b>A</b>" and "<b>B</b>", which "<b>Hold</b>" the "<b>Previous Two Values</b>" needed as the "<b>Function Proceeds</b>".
# MAGIC * Then the "<b>Generator Function</b>", i.e., "<b>generateLucasSeries ()</b>" "<b>Enters</b>" into an "<b>Infinite While Loop</b>", where the "<b>Function</b>" "<b>Yields</b>" the "<b>Value</b>" of the "<b>Variable</b>" "<b>B</b>".
# MAGIC * After that, the "<b>Two Variables</b>", i.e., "<b>A</b>" and "<b>B</b>" are "<b>Updated</b>" to "<b>Hold</b>" the "<b>New Previous Two Values</b>" using a "<b>Neat Application</b>" of "<b>Tuple Unpacking</b>".
# MAGIC * When the "<b>Generator Function</b>" is "<b>Called</b>", and, the "<b>Generator Object</b>" would be "<b>Created</b>", it would be "<b>Used</b>" like any "<b>Iterable Object</b>".
# MAGIC * To "<b>Display</b>" the "<b>Lucas Numbers</b>", the "<b>for</b>" <b>Loop</b> can be used.
# MAGIC * Since, the "<b>Generator Function</b>", i.e., "<b>generateLucasSeries ()</b>" has used the "<b>Infinite While Loop</b>", the "<b>for</b>" <b>Loop</b> used to "<b>Display</b>" the "<b>Lucas Numbers</b>" will "<b>Run Forever</b>", "<b>Printing</b>" out the "<b>Values</b>" "<b>Until</b>" the "<b>Computer</b>" "<b>Runs Out of Memory</b>".

# COMMAND ----------

# DBTITLE 1,"Create" the "Infinite Generator Function", i.e., "generateLucasSeries ()"
def generateLucasSeries ():
  yield 2
  A = 2
  B = 1
  while True:
    yield B
    A, B = B, A + B

# COMMAND ----------

# DBTITLE 1,"Display" the "Lucas Numbers" Using the "Infinite Generator Function", i.e., "generateLucasSeries ()"
for lucasNum in generateLucasSeries ():
  print(lucasNum)
