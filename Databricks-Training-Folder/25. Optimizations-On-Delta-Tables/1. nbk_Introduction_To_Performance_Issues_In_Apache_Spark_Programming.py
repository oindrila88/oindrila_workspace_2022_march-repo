# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to "Performance Issues" in "Apache Spark" Programming
# MAGIC * Topic: Introduction to the "Five Most Common Performance Problems" of "Apache Spark".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Five Most Common Performance Problems" of "Apache Spark"
# MAGIC * Every "<b>Data Engineer</b>" will face various "<b>Performance Problems</b>" while "<b>Developing</b>" the "<b>Queries</b>". 
# MAGIC * "<b>How</b>" to "<b>Mitigate</b>" some of these "<b>Issues</b>" might fall to the "<b>Purview</b>" of a "<b>Senior Developer</b>", but having even a "<b>Basic Understanding</b>" of these "<b>Issues</b>", and "<b>How</b>" to "<b>Diagnose</b>" these "<b>Issues</b>", will "<b>Enable</b>" every "<b>Developer</b>" to quickly bring "<b>Resolution</b>" to their "<b>Performance Problems</b>".
# MAGIC * The "<b>Five Most Common Performance Problems</b>" with "<b>Spark Applications</b>" are -
# MAGIC <br>1. <b>Skew</b> - An "<b>Imbalance</b>" in the "<b>Size</b>" of "<b>Partitions</b>".
# MAGIC <br>2. <b>Spill</b> - The "<b>Writing</b>" of "<b>Temporary Files</b>" to "<b>Disk</b>" due to a "<b>lack of Memory</b>".
# MAGIC <br>3. <b>Shuffle</b> - The "<b>Act</b>" of "<b>Moving Data</b>" between the "<b>Executors</b>", which is a "<b>Natural Result</b>" of the "<b>Wide Transformation Operations</b>".
# MAGIC <br>4. <b>Storage</b> - A "<b>Set of Problems</b>" that are "<b>Directly Related</b>" to "<b>How Data is Stored on Disk</b>", like - "<b>Tiny Files Problems</b>".
# MAGIC <br>5. <b>Serialization</b> - The "<b>Distribution</b>" of "<b>Code Segments</b>" across the "<b>Cluster</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Multiple Performance Problems" of the "Five Most Common Performance Problems" of "Apache Spark" Can Occur at the "Same Time"
# MAGIC * Quite a few of the above-mentioned "<b>Problems</b>" can "<b>Exist</b>" at any "<b>Given Time Simultaneously</b>", "<b>Depending</b>" on the "<b>Nature</b>" of the "<b>Dataset</b>", and, the "<b>Nature</b>" of the "<b>Job</b>". In that case, "<b>Finding</b>" the  "<b>Root Source</b>" the "<b>Problems</b>" is "<b>Hard</b>" when "<b>One Problem</b>" can "<b>Cause Another</b>", like the following -
# MAGIC <br><br><b>1.</b> "<b>Skew</b>" Can Induce "<b>Spill</b>" - Because of the "<b>Imbalance</b>" of "<b>Data</b>" in the different "<b>Partitions</b>", if there is "<b>One Partition</b>" that is "<b>Too Large</b>", then that "<b>Partition</b>" can "<b>Manifest</b>" itself as being in situation regarding "<b>Out of Memory</b>" and induce "<b>Spill</b>".
# MAGIC <br>So, the "<b>Developers</b>" can see that there is a "<b>Spill</b>" occurring and "<b>Not Recognizing</b>" that the "<b>Root Cause</b>" is actually the "<b>Skew</b>".
# MAGIC <br><br><b>2.</b> "<b>Storage Issues</b>" Can Induce "<b>Excess Shuffle</b>" - If the "<b>Data</b>" is "<b>Stored Improperly</b>" on "<b>Disks</b>", then it comes to "<b>Spark</b>" in "<b>Not So Proper Format</b>" and it can lead to "<b>Increase</b>" in "<b>Degradation</b>" of "<b>Performance</b>" in a "<b>Shuffle Operation</b>".
# MAGIC <br><br><b>3.</b> "<b>Incorrectly Addressing Shuffle</b>" Can Exacerbate "<b>Skew</b>" - If the "<b>Performance Issue</b>", caused by a "<b>Shuffle Operation</b>", is tried to be "<b>Addressed</b>" by introducing "<b>Bucketing</b>", then that is going to "<b>Exasperate</b>" the "<b>Skew</b>", because the "<b>Act</b>" of "<b>Bucketing</b>" itself can "<b>Manifest</b>" itself as "<b>Skew</b>".
# MAGIC <br><br>So, by "<b>Attempting</b>" to "<b>Solve One Problem</b>", the "<b>Developers</b>" can induce "<b>Another Problem</b>" if they are "<b>Not Careful</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Benchmarking"?
# MAGIC * "<b>Benchmarking</b>" is a "<b>Diagnostic Technique</b>" to "<b>Find</b>" if there is any "<b>Performance Problem</b>" present in a "<b>Spark Application</b>". There are some "<b>Concerns</b>" that need to be taken into "<b>Considerations</b>", such as - "<b>Not Measuring</b>" the "<b>Side Effects</b>" that are "<b>Non-Related</b>" to what is being "<b>Measured</b>".
# MAGIC * In "<b>Databricks</b>", in "<b>Notebook Eenvironment</b>", people generally "<b>Attempt</b>" to "<b>Employ</b>" roughly the following "<b>Three</b>" different types of "<b>Benchmarks</b>" -
# MAGIC <br><br><b>1.</b> "<b>count () Action</b>" - In the "<b>count () Action</b>", the "<b>Developers</b>" literally takes a "<b>DataFrame</b>", then "<b>Count</b>" the "<b>Number of Records</b>" in that "<b>DataFrame</b>", and see "<b>How Long</b>" that takes.
# MAGIC <br><br><b>2.</b> "<b>foreach () Action with Do-Nothing Lambda</b>" - The "<b>foreach () Action with Do-Nothing Lambda</b>" "<b>Iterates Over</b>" the "<b>Every Single Row</b>" in a "<b>DataFrame</b>", and see "<b>How Long</b>" that takes.
# MAGIC <br><br><b>3.</b> "<b>noop Write (No-Operation Write)</b>" – The "<b>noop Write (No-Operation Write)</b>" Operation is "<b>Very Similar</b>" to the "<b>foreach () Action with Do-Nothing Lambda</b>" Operation, except that "<b>Inside</b>" the "<b>noop Write (No-Operation Write)</b>" Operation, "<b>No Lambda</b>" is "<b>Passed</b>". This Operation "<b>Emulates</b>" a "<b>Write Operation</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### count () Action Benchmark

# COMMAND ----------

# DBTITLE 1,Read a "CSV File" for the "First Time" to Perform the "count ()" Action to Establish a Benchmark
sc.setJobDescription("Step 1: Read a CSV File for the First Time and Perform the count () Action to Establish a Benchmark")

df_ReadCustomerFileWithHeader = spark.read\
                                     .option("header", "true")\
                                     .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")\
                                     .count()

# COMMAND ----------

# DBTITLE 1,Read the "Same CSV File" for the "Second Time"  to Perform the "count ()" Action to Establish a Benchmark
sc.setJobDescription("Step 2: Read the Same CSV File Again and Perform the count () Action to Establish a Benchmark")

df_ReadCustomerFileWithHeader = spark.read\
                                     .option("header", "true")\
                                     .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")\
                                     .count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why the "Time Taken" to "Read a File" for the "Second Time" is "Very Less" Compared to the "Time Taken" to "Read a File" for the "First Time"?
# MAGIC * When a "<b>DataFrame</b>" needs to "<b>Read</b>" the "<b>Schema</b>" of the "<b>File</b>" to "<b>Read</b>" for the "<b>First Time</b>" from the "<b>Source File Itself</b>", "<b>Apache Spark</b>" "<b>Executes</b>" a "<b>Separate Job</b>" for the "<b>Sole Purpose</b>" of "<b>Reading the Schema</b>".
# MAGIC * Once, the "<b>Schema</b>" is "<b>Read</b>", then "<b>Apache Spark</b>" can "<b>Continue</b>" the "<b>Analysis</b>", and, "<b>Executes</b>" the "<b>Next Job</b>", like - "<b>Finishing</b>" the "<b>Operation</b>" of "<b>Counting All</b>" the "<b>Records</b>" from the "<b>Source File</b>".
# MAGIC * Now, since the "<b>Databricks Notebook</b>" has "<b>Already Loaded</b>" the "<b>Schema</b>", and, "<b>Spark Session</b>" is also "<b>Aware</b>" of "<b>What</b>" the "<b>Schema</b>" is, when the "<b>Same Source File</b>" is "<b>Read</b>" for the "<b>Second Time</b>", even though the "<b>Job</b>" of "<b>Reading the Schema</b>" is "<b>Not Skipped</b>", it is actually "<b>Not Going</b>" to the "<b>File Storage</b>" to "<b>Read the Schema</b>" from the "<b>Source File</b>".

# COMMAND ----------

# DBTITLE 1,Read the "Same CSV File" for the "Third Time"  to Perform the "count ()" Action in "Scala" to Establish a Benchmark
# MAGIC %scala
# MAGIC sc.setJobDescription("Step 3: Read the Same CSV File Again in Scala and Perform the count () Action to Establish a Benchmark")
# MAGIC 
# MAGIC val df_ReadCustomerFileWithHeader = spark.read
# MAGIC                                          .option("header", "true")
# MAGIC                                          .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
# MAGIC                                          .count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### foreach () Action with Do-Nothing Lambda Benchmark

# COMMAND ----------

# DBTITLE 1,Read a "CSV File" and "Iterate" Over "All the Rows" to Establish a Benchmark
sc.setJobDescription("Step 4: Read a CSV File and Iterate Over All the Rows with Do-Nothing Lambda to Establish a Benchmark")

df_ReadCustomerFileWithHeader = spark.read\
                                     .option("header", "true")\
                                     .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")\
                                     .foreach(lambda x: None)

# COMMAND ----------

# DBTITLE 1,Read a "CSV File" in "Scala" and "Iterate" Over "All the Rows" to Establish a Benchmark
# MAGIC %scala
# MAGIC sc.setJobDescription("Step 5: Read a CSV File in Scala and Iterate Over All the Rows with Do-Nothing Lambda to Establish a Benchmark")
# MAGIC 
# MAGIC val df_ReadCustomerFileWithHeader = spark.read
# MAGIC                                          .option("header", "true")
# MAGIC                                          .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
# MAGIC                                          .foreach(_=> ())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why the "Time Taken" to "Perform" the "foreach () Action with Do-Nothing Lambda" is "More" Compared to the "Time Taken" to "Perform" the "count () Action"?
# MAGIC * The "<b>count () Action</b>" is actually "<b>Optimized</b>". With the "<b>Columnar Format</b>", such as "<b>Parquet</b>", it is actually possible to "<b>Determine</b>" the "<b>Total Number of Records</b>" "<b>Without Reading All the Data</b>", because, it is possible to "<b>Scan</b>" on a "<b>Single Column</b>", "<b>Count</b>" the "<b>Number of Rows</b>" of that "<b>Particular Column</b>", and, "<b>Return</b>" the "<b>Result</b>" as an "<b>Operation</b>".
# MAGIC * The "<b>foreach () Action with Do-Nothing Lambda</b>" is "<b>Not Optimized</b>". This operation "<b>Pulls In Every Single Record</b>" from the "<b>Source File</b>" into the "<b>Executor</b>", and, "<b>Iterate Over Every Single Record</b>".
# MAGIC <br>As the "<b>foreach () Action with Do-Nothing Lambda</b>" operation is "<b>Iterating Over Every Single Record</b>", this operation is "<b>Incurring</b>" the "<b>Cost</b>" of "<b>Pulling the Data</b>" into the "<b>Spark Engine</b>".
# MAGIC <br>This is the reason the "<b>foreach () Action with Do-Nothing Lambda</b>" operation is "<b>Taking Longer</b>" to "<b>Finish</b>" Compared to the "<b>count () Action</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why the "Time Taken" to "Perform" the "foreach () Action with Do-Nothing Lambda" in "Python" is "More" Compared to the "Time Taken" to "Perform" the "foreach () Action with Do-Nothing Lambda" in "Scala"?
# MAGIC * The "<b>foreach () Action</b>" is taking a "<b>Lambda Function</b>". The "<b>Code</b>" of the "<b>Lambda Function</b>" needs to be "<b>Serialized</b>" before sending to the "<b>Executors</b>".
# MAGIC * In case of "<b>Python</b>", not only the "<b>Code</b>" of the "<b>Lambda Function</b>" needs to be "<b>Serialized</b>", but also the "<b>Python Interpreter</b>" needs to be "<b>Present</b>" to "<b>Execute</b>" the "<b>Code</b>" of the "<b>Lambda Function</b>", even if the "<b>Code</b>" of the "<b>Lambda Function</b>" technically "<b>Doesn't Do Anything</b>".
# MAGIC * So the "<b>Overhead</b>" is "<b>Not Because</b>" of the "<b>Slowness</b>" of the "<b>Python DataFrame</b>", but, "<b>Because</b>" of the "<b>Impirical Code</b>" having to "<b>Perform Serialization</b>", which comes with a "<b>Huge Penalty</b>" on "<b>Python</b>". 

# COMMAND ----------

# MAGIC %md
# MAGIC ### noop Write (No-Operation Write) Benchmark

# COMMAND ----------

# DBTITLE 1,Read a "CSV File", "Iterate" Over "All the Rows", and, Use a "noop Write" to Establish a Benchmark
sc.setJobDescription("Step 6: Read a CSV File, Iterate Over All the Rows, and, Use a noop Write to Establish a Benchmark")

df_ReadCustomerFileWithHeader = spark.read\
                                     .option("header", "true")\
                                     .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")\
                                     .write.format("noop").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Read a "CSV File" in "Scala", "Iterate" Over "All the Rows", and, Use a "noop Write" to Establish a Benchmark
# MAGIC %scala
# MAGIC sc.setJobDescription("Step 7: Read a CSV File in Scala, Iterate Over All the Rows, and, Use a noop Write to Establish a Benchmark")
# MAGIC 
# MAGIC val df_ReadCustomerFileWithHeader = spark.read
# MAGIC                                          .option("header", "true")
# MAGIC                                          .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
# MAGIC                                          .write.format("noop").mode("overwrite").save()
