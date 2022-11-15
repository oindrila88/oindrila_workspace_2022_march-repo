# Databricks notebook source
# MAGIC %md
# MAGIC # "Execution Plan" in Databricks
# MAGIC * Topic: Introduction to "Execution Plan"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Execution Plan"?
# MAGIC * The "<b>Execution Plan</b>" in "<b>Databricks</b>" Allows the "<b>Users</b>" to "<b>Understand</b>" "<b>How</b>" the "<b>Codes</b>" will "<b>Actually</b>" get "<b>Executed</b>" Across a "<b>Cluster</b>" and is "<b>Useful</b>" for "<b>Optimising</b>" the "<b>Queries</b>".
# MAGIC <br>The "<b>Execution Plan</b>" "<b>Translates</b>" the "<b>Operations</b>" into "<b>Optimized Logical Plan</b>" and "<b>Physical Plans</b>" and "<b>Shows What Operations</b>" are Going to be "<b>Executed</b>" and "<b>Sent</b>" to the "<b>Spark Executors</b>".
# MAGIC * The "<b>Execution Plan</b>" is "<b>Made</b>" of "<b>Logical Plans</b>" and "<b>Physical Plans</b>".
# MAGIC <br><img src = '/files/tables/images/execution_plan.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Different Sections of "Logical Plans"
# MAGIC * The "<b>Logical Plan</b>" is "<b>Broken Down</b>" into "<b>Three Sections</b>" -
# MAGIC * 1. <b>Parsed Logical Plan</b> (<b>Unresolved Logical Plan</b>): The "<b>Parsed Logical Plan</b>", or, "<b>Unresolved Logical Plan</b>" is "<b>Created</b>" with the "<b>Flow of Execution</b>" <b>Once</b> the "<b>Code</b>" has been "<b>Validated</b>", and, where "<b>Apache Spark</b>" is "<b>Unable</b>" to "<b>Validate</b>" a "<b>Table</b>" or "<b>Column</b>" Objects "<b>Successfully</b>", it "<b>Flags</b>" those "<b>Table</b>" or "<b>Column</b>" Objects as "<b>Unresolved</b>".
# MAGIC * 2. <b>Analyzed Logical Plan</b> (<b>Resolved Logical Plan</b>): Using the "<b>Metadata Catalog</b>", "<b>Apache Spark</b>" "<b>Validates</b>" and "<b>Resolves</b>" the "<b>Unresolved Table</b> or <b>Column</b>" Objects, which were "<b>Identified</b>" in the "<b>Unresolved Logical Plan</b>" before continuing.
# MAGIC * 3. <b>Optimized Logical Plan</b>: Once everything is "<b>Resolved</b>", the "<b>Resolved Logical Plan</b>" is "<b>Sent</b>" to the "<b>Catalyst Optimizer</b>", which "<b>Applies</b>" the "<b>Predicates</b>" or "<b>Rules</b>", and, "<b>Creates</b>" the "<b>Various Optimization Strategies</b>" to further "<b>Optimize</b>" the "<b>Resolved Logical Plan</b>".
# MAGIC <br>"<b>Optimize Rules</b>" can "<b>Consist</b>" of "<b>Predicate</b> or <b>Projection Pushdown</b>", "<b>Reordering Operations</b>", "<b>Conversions</b>" and "<b>Simplifying Expressions</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Physical Plan"?
# MAGIC * The "<b>Physical Plan</b>" is "<b>How</b>" the "<b>Optimized Logical Plan</b>" that was "<b>Created</b>", is going be "<b>Executed</b>" on the "<b>Cluster</b>".
# MAGIC * The "<b>Catalyst Optimizer</b>" "<b>Generates</b>" the "<b>Multiple Physical Plans</b>" based on the "<b>Various Optimization Strategies</b>".
# MAGIC * Each of the "<b>Various Optimization Strategies</b>" is "<b>Assessed</b>" through a "<b>Cost Model</b>", establishing the "<b>Estimates</b>" for "<b>Execution Time</b>" and "<b>Resources Utilisation</b>".
# MAGIC * Using the "<b>Cost Model Estimates</b>", the "<b>Catalyst Optimizer</b>" determines the "<b>Best Optimal Plan</b>/<b>Strategy</b>" and "<b>Selects</b>" it as the "<b>Selected Physical Plan</b>", which is "<b>Executed</b>" on the "<b>Cluster</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Catalyst Optimizer"?
# MAGIC * The "<b>Catalyst Optimizer</b>" is an "<b>Extensible Query Optimizer</b>".
# MAGIC * The "<b>Catalyst Optimizer</b>" is at the "<b>Core</b>" of the "<b>Spark SQL's</b>" "power and speed.
# MAGIC * The "<b>Catalyst Optimizer</b>" "<b>Automatically Finds</b>" the "<b>Most Efficient Plan</b>" for "<b>Applying</b>" the "<b>Transformations</b>" and "<b>Actions</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "<b>Catalyst Optimizer</b>" "<b>Optimizes</b>" "Queries"?
# MAGIC * The following "<b>Stages</b>" occur as the "<b>Input Query</b>" travels through the "<b>Optimization Process</b>" -
# MAGIC * 1. <b>User Input</b> - Using "<b>Declarative APIs</b>" of "<b>Spark SQL</b>", "<b>DataFrame</b>", or "<b>Dataset</b>", the users can specify the "<b>Queries</b>" to be processed.
# MAGIC * 2. <b>Unresolved Logical Plan</b> - In the "<b>Unresolved Logical Plan</b>" Phase, "<b>Apache Spark</b>" accepts the “Query” that a "<b>User Writes</b>", and, "<b>Creates</b>" an "<b>Unresolved Logical Plan</b>" with the "<b>Flow of Execution</b>", which needs to be "<b>Validated</b>" against the "<b>Metadata Catalog</b>" in the "<b>Next Step</b>" to be "<b>Sure</b>" that there "<b>Won't</b>" be any "<b>Naming</b>, or, "<b>Data Type</b>" Errors in "<b>Tables</b>", "<b>Columns</b>", "<b>UDFs</b>" etc.
# MAGIC <br> The "<b>Metadata Catalog</b>" is a "<b>Metadata Repository</b>" of "<b>All</b>" the "<b>Table</b>" and "<b>DataFrames</b>".
# MAGIC * 3. <b>Analysis</b> - In the "<b>Analysis</b>" Phase, "<b>Apache Spark</b>" "<b>Validates</b>" the "<b>Unresolved Logical Plan</b>" against the "<b>Metadata Catalog</b>", and, then turns it into a "<b>Resolved Logical Plan</b>".
# MAGIC * 4. <b>Logical Optimization</b> - In the "<b>Logical Optimization</b>" Phase the "<b>First Set</b>" of "<b>Optimizations</b>" take place, i.e., the "<b>Catalyst Optimizer</b>" "<b>Adjusts</b>" the "<b>Resolved Logical Plan</b>" by "<b>Applying</b>" the "<b>Predicates</b>" or "<b>Rules</b>", and, "<b>Creating</b>" the "<b>Various Optimization Strategies</b>" to make the "<b>Resolved Logical Plan</b>" as efficient as possible.
# MAGIC * 5. <b>Physical Planning</b> - In the "<b>Physical Planning</b>" Phase, the "<b>Catalyst Optimizer</b>" "<b>Generates</b>" "<b>One</b>" or More "<b>Physical Plans</b>" of "<b>Executing</b>" a "<b>Query</b>". For Example - 
# MAGIC <br>A. Is the "<b>Catalyst Optimizer</b>" going to "<b>Pull</b> "<b>All</b>" of the "Data", i.e., "<b>100%</b>" of the "Data" across the "<b>Network</b>", or,
# MAGIC <br>B. Is the "<b>Catalyst Optimizer</b>" going to "<b>Use</b>" a "<b>Predicate Pushdown</b>" and "<b>Filter</b>" the "Data" at its "<b>Source</b>", like a "<b>Parquet File</b>", or, "<b>JDBC"</b>. Thus, "<b>Bringing Over</b>" may be "<b>Only 30%</b>" of the "Data".
# MAGIC <br>Each of the "<b>Physical Plans</b>" represents "<b>What</b>" the "<b>Query Engine</b>" will "<b>Actually Do</b>" after "<b>All</b>" of the "<b>Optimizations</b>" have been "<b>Applied</b>".
# MAGIC * 6. "<b>Cost Model</b> - Each of the "<b>Optimizations</b>" in Each of the "<b>Physical Plans</b>" provides a "<b>Measurably Different Benefit</b>". This is the "<b>Optimization’s Cost Model</b>".
# MAGIC <br>In this Phase, each "<b>Physical Plan</b>" is "<b>Evaluated</b>" according to its Own "<b>Cost Model</b>". The "<b>Best Performing Model</b>" is "<b>Selected</b>". This provides the "<b>Selected Physical Plan</b>".
# MAGIC * 7. <b>Whole Stage Code Generation</b> - In the "<b>Whole Stage Code Generation</b>" Phase, the "<b>Catalyst Optimizer</b>" "<b>Converts</b>" the "<b>Selected Physical Plan</b>" to "<b>RDDs</b>", and, then "<b>Generates</b>" the "<b>Bytecodes</b>", so that the "<b>Selected Physical Plan</b>" is "<b>Executed</b>" on the "<b>Cluster</b>".
# MAGIC <br><img src = '/files/tables/images/catalyst_optimizer.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Limitation of "Catalyst Optimizer"
# MAGIC * The "<b>Catalyst Optimizer</b>" in "<b>Spark 2.x</b>" "Applies" the "<b>Optimizations</b>" throughout "<b>Logical Optimization Stage</b>" and "<b>Physical Planning Stage</b>". It "<b>Generates</b>" a "<b>Selection</b>" of "<b>Physical Plans</b>" and "<b>Selects</b>" the "<b>Most Efficient One</b>".
# MAGIC * These are "<b>Rule-Based Optimizations</b>", and, while these generally "<b>Improve</b>" the "<b>Query Performances</b>", these are all "<b>Based</b>" on "<b>Estimates</b>" and "<b>Statistics</b>" that are "<b>Generated Before Runtime</b>". Hence, there "<b>May</b>" be "<b>Unanticipated Problems</b>", or, "<b>Tuning Opportunities</b>" appearing as the "<b>Query Runs</b>".
