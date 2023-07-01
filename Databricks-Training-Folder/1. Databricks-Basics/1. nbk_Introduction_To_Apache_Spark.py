# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark Basics
# MAGIC * Topic: Introduction to "Apache Spark"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Apache Spark"?
# MAGIC * At a "<b>High Level</b>", "<b>Apache Spark</b>" is a "<b>Lightening-Fast</b>", and, "<b>Unified</b>" "<b>Analytics Engine</b>" for "<b>Big Data Processing</b>", and, "<b>Machine Learning</b>".
# MAGIC * "<b>Spark</b>" was "<b>Originally Developed</b>" at "<b>UC Berkeley</b>" in "<b>2009</b>", and, was made "<b>Open-Sourced</b>" in "<b>2010</b>" under the "<b>BSD License</b>".
# MAGIC * In "<b>2013</b>", "<b>Spark</b>" was "<b>Donated</b>" to the "<b>Apache Software Foundation</b>", and, "<b>Switched</b>" its "<b>License</b>" to "<b>Apache 2.0</b>".
# MAGIC * "<b>Apache Spark</b>" is used by the "<b>Internet Giants</b>", such as - "<b>Yahoo</b>", "<b>eBay</b>", and, "<b>Netflix</b>" for "<b>Large Scale Data Processing</b>" on "<b>Multiple Petabytes</b>" of "<b>Data</b>" on "<b>Clusters</b>" of "<b>Thousand Nodes</b>".
# MAGIC * "<b>Apache Spark</b>" was "<b>Built</b>" from the "<b>Ground Up</b>" to "<b>Address</b>" the "<b>Shortcomings</b>" of "<b>Hadoop</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Shortcomings" of "Hadoop"
# MAGIC * Following are the "<b>Shortcomings</b>" of "<b>Hadoop</b>" -
# MAGIC   * "<b>Hadoop</b>" was "<b>Slow</b>", and, "<b>Inefficient</b>" for "<b>Interactive</b>", and, "<b>Iterative</b>" "<b>Computing Jobs</b>".
# MAGIC   * "<b>Hadoop</b>" was "<b>Too Complex</b>" to "<b>Learn</b>", and, "<b>Develop</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Advantages" of "Apache Spark"
# MAGIC * "<b>Apache Spark</b>" "<b>Offers</b>" a "<b>Much Simpler</b>", "<b>Faster</b>", and, "<b>Easier APIs</b>" to "<b>Develop On</b>".
# MAGIC * "<b>Apache Spark</b>" can be "<b>100 Times Faster</b>" than "<b>Hadoop</b>" for "<b>Large Scale Data Processing</b>" by "<b>Exploiting</b>" the "<b>In-Memory Computing</b>", and, "<b>Other Optimizations</b>".
# MAGIC * "<b>Similar</b>" to "<b>Most</b>" of the "<b>Other Analytics Engines</b>" for "<b>Big Data Processing</b>", "<b>Apache Spark</b>" "<b>Runs</b>" on a "<b>Completely Distributed Computing Platform</b>".
# MAGIC * "<b>Apache Spark</b>" has a "<b>Unified Engine</b>" to "<b>Support</b>" the "<b>Varying Workloads</b>".
# MAGIC <br>Example - "<b>Apache Spark</b>" uses a "<b>Single Engine</b>" for "<b>Streaming</b>" and "<b>Batch</b>" "<b>Workloads</b>". It "<b>Does Not Have Separate Engine</b>" for "<b>Each</b>" of the "<b>Workloads</b>".
# MAGIC * "<b>Apache Spark</b>" comes "<b>Packaged</b>" with "<b>High-Level Libraries</b>", including the "<b>Support</b>" for "<b>SQL Queries</b>", "<b>Streaming data</b>", "<b>Machine Learning</b>", and, "<b>Graph Processing</b>".
# MAGIC <br>These "<b>Standard Libraries</b>" "<b>Increase</b>" the "<b>Developer's Productivity</b>", and, can be "<b>Seamlessly Combined</b>" to "<b>Create</b>" te "<b>Complex Workflows</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Architecture" of "Apache Spark"
# MAGIC * <b>1</b>. <b>Spark Core</b> - At the "<b>Center</b>" of "<b>Spark Architecture</b>" is "<b>Spark Core</b>", which "<b>Contains</b>" the "<b>Basic Functionalities</b>" of "<b>Apache Spark</b>".
# MAGIC <br>The "<b>Spark Core</b>" "<b>Takes Care</b>" of "<b>Scheduling Tasks</b>", "<b>Memory Management</b>" for "<b>Recovery</b>", "<b>Communication with Storage Systems</b>" etc.
# MAGIC <br>The "<b>Spark Core</b>" is also "<b>Home</b>" to the "<b>Apache Spark's Main Programming Abstraction API</b>", called "<b>RDD</b>", i.e., "<b>Resilient Distributed Dataset</b>".
# MAGIC <br><img src = '/files/tables/images/spark_core_image.jpg'>
# MAGIC * <b>2</b>. <b>Spark SQL Engine</b> - In order to "<b>Optimize</b>" the "<b>Workloads</b>", "<b>Apache Spark</b>" "<b>Introduced</b>" the "<b>Second Engine</b>", called the "<b>Spark SQL Engine</b>".
# MAGIC <br>The "<b>Spark SQL Engine</b>" "<b>Includes</b>" the following -
# MAGIC   * <b>Catalyst Optimizer</b> - The "<b>Catalyst Optimizer</b>" "<b>Takes Care</b>" of "<b>Converting</b>" a "<b>Computational Query</b>" to a "<b>Highly Efficient Execution Plan</b>".
# MAGIC   * <b>Tungsten Project</b> - The "<b>Tungsten Project</b>" is "<b>Responsible</b>" for the "<b>Memory Management</b>", and, the "<b>CPU Efficiency</b>".
# MAGIC <img src = '/files/tables/images/spark_sql_engine_image.jpg'>
# MAGIC * <b>3</b>. <b>Higher Level APIs</b> - The "<b>Higher Level Abstraction APIs</b>", such as - "<b>Spark SQL</b>", "<b>DataFrame API</b>", and, "<b>Dataset API</b>" "<b>Make It Easier</b>" to "<b>Develop</b>" the "<b>Applications</b>", and, also "<b>Benefit</b>" from the "<b>Optimizations</b>" from the "<b>Spark SQL Engine</b>".
# MAGIC <br>The "<b>Recommended Approach</b>" to "<b>Develop</b>" the "<b>Applications</b>" in "<b>Apache Spark</b>" is to "<b>Use</b>" these "<b>Higher Level Abstraction APIs</b>", rather than the "<b>APIs</b>" to "<b>Create</b>", and, "<b>Manipulate</b>" these "<b>RDD Collections</b>".
# MAGIC <br>The "<b>DataFrame API</b>", and, "<b>Dataset API</b>" can be "<b>Invoked</b>" from "<b>Any</b>" of the "<b>Domain Specific Language</b>", such as - "<b>Scala</b>", "<b>Python</b>", "<b>Java</b>", or, "<b>R</b>".
# MAGIC <br>On "<b>Top</b>" of the "<b>DataFrame API</b>", and, "<b>Dataset API</b>", there is a "<b>Set</b>" of "<b>Libraries</b>", such as - "<b>Spark Structured Streaming</b>" for "<b>Streaming</b>", "<b>Spark ML Library</b>" for "<b>Machine Learning</b>", and, also "<b>Spark Graphics</b>" for "<b>Graph Processing</b>".
# MAGIC <br><img src = '/files/tables/images/higher_level_api_image.jpg'>
# MAGIC * <b>4</b>. <b>Resource Manager</b> - "<b>Apache Spark</b>" comes with its "<b>Stand-Alone Resource Manager</b>". But, "<b>Other Resource Managers</b>" can also be "<b>Chosen</b>", such as - "<b>YARN</b>", "<b>Apache Mesos</b>", and, "<b>Kubernetes</b>".
# MAGIC <br><img src = '/files/tables/images/resource_manager_image.jpg'>
# MAGIC * "<b>Combining</b>" "<b>All</b>" of the "<b>Above Mentioned</b>" "<b>Four Components</b>", "<b>Apache Spark</b>" "<b>Provides</b>" the "<b>Unified Platform</b>" for "<b>Processing</b>" the "<b>Streaming Data</b>", "<b>Batch Data</b>", "<b>Machine Learning</b>", and, "<b>Graph Processing</b>" "<b>Workloads</b>" using a "<b>Single Execution Engine</b>", and, a "<b>Standard</b>" "<b>Set</b>" of "<b>APIs</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Resilient Distributed Dataset" (RDD)
# MAGIC * "<b>RDDs</b>", i.e., "<b>Resilient Distributed Datasets</b>" are a "<b>Collection</b>" of "<b>Items</b>" that are "<b>Distributed Accross</b>" the "<b>Various Compute Nodes</b>" in a "<b>Cluster</b>" that can be "<b>Processed</b>" in "<b>Parallel</b>".
# MAGIC * "<b>Spark Core</b>" "<b>Provides</b>" the "<b>APIs</b>" to "<b>Create</b>", and, "<b>Manipulate</b>" these "<b>RDD Collections</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Disadvantages" of "Using" the "Resilient Distributed Dataset" (RDD)
# MAGIC * "<b>Early Developments</b>" in "<b>Apache Spark</b>" was "<b>Done</b>" using the "<b>APIs</b>" to "<b>Create</b>", and, "<b>Manipulate</b>" the "<b>RDD Collections</b>", but, it had the following "<b>Drawbacks</b>" -
# MAGIC   * It was "<b>Difficult</b>" to "<b>Use</b>" the "<b>RDDs</b>" for the "<b>Complex Operations</b>".
# MAGIC   * It was "<b>Difficult</b>" to "<b>Optimize</b>" the "<b>Spark</b>", and, the "<b>Responsibility</b>" "<b>Mainly Fell Down</b>" to the "<b>Developer</b>" to "<b>Write</b>" the "<b>Optimized Code</b>".
