# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to "Skew" in "Apache Spark" Programming
# MAGIC * Topic: Introduction to "Skew" as "One of the Common Performance Problems" of "Apache Spark".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Skew"?
# MAGIC * Typically "<b>Data</b>" is "<b>Read</b>" in "<b>128 MB Partitions</b>" and "<b>Evenly Distributed</b>" througout the "<b>Cluster</b>".
# MAGIC * There are "<b>Configuration Paramreters</b>" that "<b>Allow</b>" the "<b>Developers</b>" to "<b>Change</b>" the "<b>Default Size</b>" of the "<b>Partitions</b>" througout the "<b>Cluster</b>" as well, like - "<b>maxPartitionBytes</b>".
# MAGIC * However, as the "<b>Data</b>" is "<b>Transformed</b>" with time, like "<b>Aggregated</b>", it is possible to have significantly "<b>More Records</b>" in "<b>One Partition Than Others</b>".
# MAGIC * This <b>Situation</b>, where "<b>One Partition</b>" has "<b>More Records Than Others</b>", is called "<b>Skew</b>".
# MAGIC * To some degrees, a "<b>Small Amount</b>" of "<b>Skew</b>" is "<b>Ignorable</b>", may be like in "<b>10% Range</b>".
# MAGIC * "<b>Large Amount</b>" of "<b>Skews</b>" can result in "<b>Spill</b>", or, worse, really "<b>Hard to Diagnose</b>" "<b>Out of Memory (OOM) Errors</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Ramification" of "Skew"
# MAGIC <img src = '/files/tables/images/skew_image.jpg'><br>
# MAGIC * Suppose, initially, there is a "<b>Dataset</b>", having "<b>Four Partitions</b>" with roughly "<b>128 MB</b>" of "<b>Data</b>" in "<b>Each</b>" of the "<b>Partitions</b>".
# MAGIC * If the "<b>Dataset</b>" is to be "<b>Grouped By</b>" the "<b>Cities</b>", there would be an "<b>Anomaly</b>", in which, "<b>Three Cities</b>" are roughly of the "<b>Same Size</b>", whereas, the "<b>Fourth City</b>" is almost "<b>Twice the Size</b>" of the "<b>Other Three Cities</b>".
# MAGIC * That means, after "<b>Grouping By the Cities</b>", the "<b>Data</b>" in the "<b>Dataset</b>" is "<b>No Longer Evenly Distributed</b>". The "<b>Partition</b>" for "<b>City D</b>" is significantly "<b>Larger</b>" and "<b>Requires More RAM</b>" than the "<b>Other Cities</b>".
# MAGIC * So, "<b>City D</b>", being "<b>Two Times Larger</b>" than the "<b>Other Cities</b>" -
# MAGIC <br>1. Assuming that it takes "<b>N Seconds Per Record</b>" to "<b>Process</b>" the "<b>Data</b>" by "<b>Other Cities</b>", "<b>City D</b>" takes "<b>Two Times as Long</b>", i.e., "<b>2N Seconds Per Record</b>" to "<b>Process</b>" the "<b>Data</b>".
# MAGIC <br>2. Assuming that it takes "<b>N MB RAM Per Record</b>" to "<b>Process</b>" the "<b>Data</b>" by "<b>Other Cities</b>", "<b>City D</b>" takes "<b>Twice as Much RAM</b>", i.e., "<b>2N MB RAM Per Record</b>" to "<b>Process</b>" the "<b>Data</b>".
# MAGIC * So, the "<b>Ramification</b>" of "<b>Skew</b>" is as following -
# MAGIC <br>1. The "<b>Entire Stage</b>" is going to take as long as the "<b>Longest Running Task</b>". So, purely from an "<b>Execution Standpoint in Time</b>", "<b>Processing Data</b>" in the "<b>Partition</b>" of "<b>City D</b>" is going to take the "<b>Longest</b>" in the "<b>Entire Job</b>".
# MAGIC <br>2. The more dire problem would be that there might "<b>Not</b>" be "<b>Enough RAM</b>" for the "<b>Skewed Partitions</b>". The "<b>Size</b>" of the "<b>Cluster</b>" might have been "<b>Determined</b>" keeping in mind the "<b>Average Size</b>" of "<b>Data</b>" to be "<b>Processed</b>" in the "<b>Partitions</b>" of "<b>City A</b>", "<b>City B</b>", or, "<b>City C</b>".
# MAGIC <br>So, when the "<b>Data</b>" is "<b>Skewed</b>" in the "<b>Partition</b>" of "<b>City D</b>", trying to "<b>Process</b>" the "<b>Data</b>" in that "<b>Partition</b>" would "<b>Exceed the Maximum Capacity</b>" of the "<b>Executors</b>", in terms of "<b>Handling</b>" the "<b>RAM Required to Process</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Manifestation" of the "Skew"
# MAGIC * When the "<b>Skew</b>" in a "<b>Dataset</b>" is significant enough to cause a "<b>Performance Problem</b>", the "<b>Skew</b>" generally "<b>Manifests</b>" itself in one of the two ways -
# MAGIC <br>1. <b>Time</b> - The "<b>Skew</b>" can "<b>Manifest</b>" itself as "<b>Time</b>", i.e., "<b>Processing</b>" the "<b>Data</b>" in a particular "<b>Set of Task</b>", or, "<b>One Task</b>" out of a "<b>Larger Set of Tasks</b>" takes "<b>Longer</b>" than "<b>Expected</b>".
# MAGIC <br>2. <b>RAM</b> - The "<b>Skew</b>" can represent itself as a "<b>Problem</b>" with the "<b>RAM</b>", where "<b>Spill</b>" might occur, or, in the "<b>Worst Case Scenario</b>" the "<b>Out of Memory (OOM)</b>" might occurs.
# MAGIC <br>This problem should "<b>Not</b>" be the "<b>First Thing</b>" to "<b>Solve</b>".
# MAGIC * The "<b>First Problem</b>" to "<b>Solve</b>" is the "<b>Uneven Distribution of Records Across All the Partitions</b>", because, for example, if the "<b>Developer</b>" tries to "<b>Solve</b>" the "<b>Problem</b>" for "<b>Time</b>"/"<b>Speed</b>", or, "<b>RAM</b>", by "<b>Throwing Some Moew Memory</b>", the "<b>Root Problem</b>" would "<b>Still</b>" be "<b>There</b>". In that case, the "<b>Developer</b>" will be able to "<b>Treat</b>" the "<b>Symptom</b>", but, "<b>Not</b>" the "<b>Root Cause</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to "Mitigate" the "Skew"?
# MAGIC * There are "<b>Several Different Strategies</b>" available for "<b>Fixing</b>" the "<b>Skew</b>" -
# MAGIC * 1. "<b>Employ</b>" a "<b>Databricks-Specific Skew Hint</b>", i.e., "<b>Skew Join Optimization</b>".
# MAGIC * 2. If "<b>Spark 3.0</b>" is used, the "<b>Adaptive Query Execution (AQE)</b>" can be "<b>Enabled</b>".
# MAGIC <br>"<b>Adaptive Query Execution (AQE)</b>" will "<b>Address</b>" the "<b>Skew Problem</b>" by "<b>Balancing Out</b>" the "<b>Partitions Automatically</b>".
# MAGIC * 3. "<b>Salt</b>" the "<b>Skewed Column</b>" with a "<b>Random Number</b>", and, by doing that, the "<b>Data</b>" is "<b>Distributed</b>" in a "<b>Better Manner Across Each Partition</b>", but, at the "<b>Cost</b>" of some "<b>Extra Processing</b>".

# COMMAND ----------

# DBTITLE 1,Basic Initialization
sc.setJobDescription("Step 1: Basic Initialization")

# "Factor" of "8 Cores" and "Greater Than" the "Expected 825 Partitions"
spark.conf.set("spark.sql.shuffle.partitions", 832)

# Disabled to "Avoid" the "Side Effects"
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# Define the "Path" of the "Two Datasets" to "Create"
cityPath = 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/cities/all.delta'
transactionPath = 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta'

# COMMAND ----------

# DBTITLE 1,Display "How" the "Data" is "Skewed"?
sc.setJobDescription("Step 2: How Skewed?")

dfVisualize = spark.read\
                   .format("delta")\
                   .load(transactionPath)\
                   .groupBy("city_id")\
                   .count()\
                   .orderBy("count")

display(dfVisualize)

# COMMAND ----------

# DBTITLE 1,"Ensure" a "Baseline" by "Disabling" the "AQE" Only
sc.setJobDescription("Step 3: Ensure a Baseline by Disabling the AQE Only")

# Ensure that "Adaptive Query Execution (AQE)" is Disabled
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "false")

#Load the City DataFrame
dfCity = spark.read\
              .format("delta")\
              .load(cityPath)

#Load the Transaction DataFrame
dfTransaction = spark.read\
                     .format("delta")\
                     .load(transactionPath)

#Join the Two DataFrames by "city_id" and Execute a "noop" Write
dfJoin = dfTransaction\
                      .join(dfCity, dfTransaction["city_id"] == dfCity["city_id"])\
                      .write\
                      .format("noop")\
                      .mode("overwrite")\
                      .save()

display(dfJoin)

# COMMAND ----------

# DBTITLE 1,"Ensure" a "Baseline" by "Disabling" the "AQE" and "Enabling" the "Skew Join Hint"
sc.setJobDescription("Step 4: Ensure a Baseline by Disabling the AQE and Enabling the Join with Skew Hint")

# Ensure that "Adaptive Query Execution (AQE)" is Disabled
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "false")

#Load the City DataFrame
dfCity = spark.read\
              .format("delta")\
              .load(cityPath)

#Load the Transaction DataFrame
dfTransaction = spark.read\
                     .format("delta")\
                     .load(transactionPath)\
                     .hint("skew", "city_id")

#Join the Two DataFrames by "city_id" and Execute a "noop" Write
dfJoin = dfTransaction\
                      .join(dfCity, dfTransaction["city_id"] == dfCity["city_id"])\
                      .write\
                      .format("noop")\
                      .mode("overwrite")\
                      .save()

display(dfJoin)
