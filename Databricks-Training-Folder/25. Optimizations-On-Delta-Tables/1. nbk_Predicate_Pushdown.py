# Databricks notebook source
# MAGIC %md
# MAGIC # What is "Predicate Pushdown" in Databricks
# MAGIC * Topic: Introduction to "Predicate Pushdown"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Predicate"?
# MAGIC * A "<b>Predicate</b>" is a "<b>Condition</b>" on a "<b>Query</b>" that Returns "<b>True</b>" or "<b>False</b>", typically "<b>Located</b>" in the "<b>WHERE</b>" Clause.

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "Predicate Pushdown" is Required?
# MAGIC * "<b>Apache Spark</b>" is a "<b>Distributed Computing Framework</b>" that is designed to work on "<b>Massive Amounts</b>" of "<b>Data</b>".
# MAGIC * The "<b>Driver</b>" Program of "<b>Apache Spark</b>" actually "<b>Splits</b>" the "<b>Overall Query</b>" into "<b>Tasks</b>" and sends these "<b>Tasks</b>" to "<b>Executor</b>" Processes on different "<b>Nodes</b>" of the "<b>Cluster</b>".
# MAGIC * To "<b>Improve</b>" the "<b>Query Performance</b>", one "<b>Strategy</b>" is to "<b>Reduce</b>" the "<b>Amount</b>" of "<b>Data</b>" that is "<b>Transferred</b>" from the "<b>Data Storage</b>" to the "<b>Executor</b>" Processes on different "<b>Nodes</b>" of the "<b>Cluster</b>".
# MAGIC * "<b>One Way</b>" to "<b>Prevent</b>" the "<b>Loading</b>" of the "<b>Data</b>" that is "<b>Not Actually Needed</b>" is "<b>Filter Pushdown</b>" (sometimes also referred to as "<b>Predicate Pushdown</b>")

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Predicate Pushdown"?
# MAGIC * A "<b>Predicate Pushdown</b>" is a "<b>Query Optimization Technique</b>" that "<b>Enables</b>" the "<b>Execution</b>" of "<b>Certain Filters</b>" at the "<b>Data Source</b>" before the "<b>All</b>" the "<b>Data</b>" is "<b>Loaded</b>" from the "<b>Data Source</b>" to an "<b>Executor</b>" Process.
# MAGIC * This becomes "<b>Even More Important</b>" if the "<b>Executors</b>" are "<b>Not</b>" on the "<b>Same Physical Machine</b>" as the "<b>Data</b>".
# MAGIC * In many cases, "<b>Predicate Pushdown</b>", or, "<b>Filter Pushdown</b>" is "<b>Automatically Applied</b>" by "<b>Apache Spark</b>" "<b>Without Explicit Commands</b> or <b>Input</b>" from the "<b>User</b>".
