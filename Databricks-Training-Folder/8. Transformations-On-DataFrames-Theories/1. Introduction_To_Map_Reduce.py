# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To MapReduce
# MAGIC * Topic: What is "MapReduce"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "MapReduce" is "Required"?
# MAGIC * "<b>Programmers</b>" have been "<b>Writing</b>" the "<b>Codes</b>" for "<b>Parallel Programming</b>" for a long time in "<b>Different Languages</b>" like - "<b>C++</b>", "<b>Java</b>", "<b>C#</b>", "<b>Python</b>". But, "<b>Maintaining</b>" the "<b>Codes</b>" is the "<b>Programmer's Responsibility</b>".
# MAGIC * There are "<b>Chances</b>" of "<b>Application Crashing</b>", "<b>Performance Hit</b>", "<b>Incorrect Results</b>". Also, such "<b>Systems</b>" if grows very large is not very fault tolerant or difficult to maintain.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "MapReduce"
# MAGIC * "<b>MapReduce</b>" is a "Java-Based", "Distributed Execution Framework" within the "<b>Apache Hadoop Ecosystem</b>".
# MAGIC * "<b>MapReduce</b>" takes away the "Complexity" of "Distributed Programming" by "Exposing" the "<b>Two Processing Steps</b>" that "Developers" Implement -
# MAGIC <br>1. <b>Map</b>
# MAGIC <br>2. <b>Reduce</b>
# MAGIC * In the "<b>Mapping</b>" Phase, the "Data" is "<b>Split</b>" between the "<b>Parallel Processing Tasks</b>" and different "<b>Transformation Logics</b>" can be "Applied" to "Each" of the "<b>Chunk of Data</b>".
# MAGIC * Once the "<b>Transformation</b>" is "<b>Completed</b>", the "<b>Reduce</b>" Phase "Takes Over" to "<b>Aggregate</b>" the "Data" from the "<b>Map Set</b>".
# MAGIC * In general, "<b>MapReduce</b>" uses "<b>Hadoop Distributed File System</b>" ("HDFS") for both the "Input" and "Output".
