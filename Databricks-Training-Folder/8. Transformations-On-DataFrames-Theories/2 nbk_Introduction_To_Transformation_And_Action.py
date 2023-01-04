# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To Transformation And Action
# MAGIC * Topic: What are "Transformation" and "Action"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "How" Does "MapReduce" Works
# MAGIC * A "MapReduce" System is usually "Composed" of the following "Three Steps" -
# MAGIC <br>1. <b>Map</b>: The "Input Data" is first "Split" into "Smaller Chunks of Data".
# MAGIC <br>The "Hadoop Framework" then "Decides" on "How Many" "<b>Mappers</b>" to "Use", based on the "Size" of the "Data" to be "Processed" and the "<b>Memory Block</b>" that is "Available" on "Each" of the "<b>Mapper Server</b>".
# MAGIC <br>"Each" of the "<b>Smaller Chunks of Data</b>" is then "<b>Assigned</b>" to a "<b>Mapper</b>" for "<b>Processing</b>".
# MAGIC <br>"Each" of the "<b>Worker Node</b>" "Applies" the "<b>map</b>" Function to the "<b>Local Data</b>", and "<b>Writes</b>" the "<b>Output</b>" to "<b>Temporary Storage</b>".
# MAGIC <br>The "Driver" then "Ensures" that "Only" a "Single Copy" of the "Redundant Input Data" is "Processed". 

# COMMAND ----------

# MAGIC %md
# MAGIC # What Are "Transformations"
# MAGIC * In "Apache Spark", the "Core Data Structures" are "Immutable", meaning these "Core Data Structures" can "Not" be "Changed" once created.
# MAGIC * Hence, in order to "Change" a "DataFrame" the "Apache Spark" needs to be "Instructed" on "How" to "Modify" the "Existing DataFrame" into the "Desired One".
# MAGIC * These "Instructions" are called "<b>Transformations</b>".
# MAGIC * "<b>Transformations</b>" are the "Core" of "How" the "<b>Business Logic</b>" are "<b>Expressed</b>" using "Apache Spark".
# MAGIC * There are "Two Types" of "<b>Transformations</b>" -
# MAGIC <br>1. <b>Narrow Dependency Transformations</b>
# MAGIC <br>2. <b>Wide Dependency Transformations</b>

# COMMAND ----------

# MAGIC %md
# MAGIC # What Are "Narrow Dependency Transformations"
# MAGIC * "<b>Narrow Dependency Transformations</b>" are those "Transformations" where "<b>Each</b>" of the "<b>Input Partition</b>" will "<b>Contribute</b>" to "<b>Only One Output Partition</b>".
# MAGIC <br><img src = '/files/tables/images/Narrow_Transformation.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Shuffle"
# MAGIC * Basically, "<b>Shuffle</b>" is a "<b>Process</b>" of "<b>Exchanging</b>" the "Data" from the "<b>Mapper</b>" to the "<b>Reducer</b>". 
# MAGIC * "<b>Operations</b>" in "<b>Apache Spark</b>" are "<b>Divided</b>" into different "<b>Stages</b>".
# MAGIC * "<b>Shuffle</b>" Operation "<b>Pushes</b>" the "Data" from "<b>One Stage</b>" to "<b>Another</b>".
# MAGIC * While "<b>Pushing</b>" the "Data" from "<b>One Stage</b>" to "<b>Another</b>", the "<b>Shuffle</b>" Operation also "<b>Partitions</b>" the "Data".
# MAGIC * "<b>Shuffle</b>" is "Implemented" in "Apache Spark" as "<b>Files on Disc</b>".
# MAGIC * There are "Two Reasons" why "<b>Shuffle</b>" is "<b>Implemented</b>" on "<b>Disc</b>". These are -
# MAGIC <br>1. It "<b>Allows</b>" the "<b>Strong Fault Tolerance Guarantees</b>" as the "Data" is "Materialized" on the "Discs". So, even if "Some Components" "Fail", it is possible to "Read" the "<b>Shuffle Data</b>".
# MAGIC <br>2. "Not All" the "Data" in a "Spark Job" can "Fit" into the "Memory".
