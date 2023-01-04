# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "finally" Block in Exception"
# MAGIC * Topic: Introduction to the "finally" Block in Exception" in Python
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "finally" Block is "Required"?
# MAGIC * Sometimes, a "<b>Cleanup Action</b>" needs to be "<b>Performed</b>", "<b>Irrespective</b>" of whether an "<b>Operation Succeeds</b>", or, "<b>Not</b>".
# MAGIC * One "<b>Solution</b>" for this is the "<b>try-finally</b>" <b>Construct</b>.

# COMMAND ----------

# MAGIC %md
# MAGIC * Consider the <b>Function</b>, i.e., "<b>createDirectoryAt</b>", which uses "<b>Various Facilities</b>" of the "<b>Standard OS Module</b>" to "<b>Change</b>" the "<b>Current Working Directory</b>", "<b>Create</b>" a "<b>New Directory</b>" at that "<b>Location</b>", and, then "<b>Restore</b>" it to the "<b>Original Working Directory</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Non Exception-Safe" Function
import os

def createDirectoryAt(path, dirName):
  originalPath = os.getcwd()
  os.chdir(path)
  os.mkdir(dirName)
  os.chdir(originalPath)

# COMMAND ----------

# DBTITLE 1,Call the Function "createDirectoryAt ()"
createDirectoryAt("/databricks/driver/FileStore/tables/training", "newDir")

# COMMAND ----------

# MAGIC %md
# MAGIC * At first sight, this seems reasonable. But, should the "<b>Call</b>" to "<b>os.mkdir ()</b>" "<b>Fails</b>" for some reason, the "<b>Current Working Directory</b>" of the "<b>Python Process</b>" "<b>Won't</b>" be "<b>Restored</b>" to its "<b>Original Value</b>", and, the <b>Function</b>, i.e., "<b>createDirectoryAt</b>" will have an "<b>Un-Intended Side-Effect</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix the Problem with "finally" Block
# MAGIC * To make the <b>Function</b>, i.e., "<b>createDirectoryAt</b>" "<b>Achieve</b>" "<b>Restoring</b>" the "<b>Original Current Working Directory</b>" under "<b>All Circumstances</b>", the "<b>try-finally</b>" <b>Block</b> is used.
# MAGIC * "<b>Code</b>" in the "<b>finally</b>" <b>Block</b> is "<b>Executed</b>" whether the "<b>Control Flow</b>" of the "<b>Program</b>" "<b>Leaves</b> the "<b>try</b>" <b>Block Normally</b> by "<b>Reaching</b>" the "<b>End</b>" of the "<b>Block</b>", or, when the "<b>Control Flow</b>" of the "<b>Program</b>" is "<b>Shifted</b>" to the "<b>except</b>" <b>Block</b>, after an "<b>Exception</b>" is "<b>Raised</b>" in the "<b>try</b>" <b>Block</b>.

# COMMAND ----------

# DBTITLE 1,Create the Function "createDirectoryAt ()" With "try-finally" Block
import os

def createDirectoryAt(path, dirName):
  originalPath = os.getcwd()
  try:
    os.chdir(path)
    os.mkdir(dirName)
  finally:
    os.chdir(originalPath)

# COMMAND ----------

# DBTITLE 1,Call the Function "createDirectoryAt ()"
createDirectoryAt("/databricks/driver/FileStore/tables/training", "newDir")

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>try-finally</b>" <b>Construct</b> can also be "<b>Combined</b>" with "<b>except</b>" <b>Blocks</b>.

# COMMAND ----------

# DBTITLE 1,Create the Function "createDirectoryAt ()" With "try-except-finally" Block
import os
import sys

def createDirectoryAt(path, dirName):
  originalPath = os.getcwd()
  try:
    os.chdir(path)
    os.mkdir(dirName)
  except OSError as e:
    print(e, file = sys.stderr)
    raise
  finally:
    os.chdir(originalPath)

# COMMAND ----------

# DBTITLE 1,Call the Function "createDirectoryAt ()"
createDirectoryAt("/FileStore/tables/training", "newDir")

# COMMAND ----------

# MAGIC %md
# MAGIC * Now if "<b>os.mkdir</b>" "<b>Raises</b>" an "<b>OS Error</b>", the "<b>Exception Handler</b>" for the "<b>OS Error</b>" will "<b>Run</b>", and, the "<b>Exception</b>" will be "<b>Re-Raised</b>".
# MAGIC * Since, the "<b>finally</b>" <b>Block</b> is "<b>Always Run</b>", "<b>No Matter How</b>" the "<b>try</b>" <b>Block</b> "<b>Ends</b>", it can be made sure that the "<b>Final Directory Change</b>" will "<b>Take Place</b>" in "<b>All Circumstances</b>".
