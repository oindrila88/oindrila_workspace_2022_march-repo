# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to "Clusters"
# MAGIC * Topic: Introduction to the "Clusters" in Databricks
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "Cluster"?
# MAGIC * A "<b>Cluster</b>" is basically a "<b>Collection</b>" of "<b>Virtual Machines</b>".
# MAGIC * In a "<b>Cluster</b>", there is usually a "<b>Driver Node</b>", which "<b>Orchestrates</b>" the "<b>Tasks</b>" that are "<b>Performed</b>" by "<b>One</b>", or, "<b>More</b>" "<b>Worker Nodes</b>".
# MAGIC <br><img src = '/files/tables/images/cluster_image.jpg'>
# MAGIC * "<b>Clusters</b>" "<b>Allow</b>" the "<b>Users</b>" to "<b>Treat</b>" the "<b>Group of Computers</b>" as a "<b>Single Compute Engine</b>" via the "<b>Driver Node</b>".
# MAGIC * "<b>Databricks Clusters</b>" "<b>Enable</b>" the "<b>Users</b>" to "<b>Run</b>" the "<b>Different Types of Workloads</b>", such as - "<b>ETL</b>" for "<b>Data Engineering</b>", "<b>Data Science Workloads</b>", "<b>Machine Learning Workloads</b>" etc.

# COMMAND ----------

# MAGIC %md
# MAGIC # "Different Types" of "Clusters" in "Databricks"
# MAGIC * "<b>Databricks</b>" "<b>Offers</b>" "<b>Two" Types</b>" of  "<b>Clusters</b>" -
# MAGIC * <b>1</b>. <b>All-Purpose Cluster</b>
# MAGIC * <b>2</b>. <b>Job Cluster</b>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "All-Purpose Cluster"?
# MAGIC * An "<b>All-Purpose Cluster</b>" can be "<b>Created Manually</b>" using the "<b>Graphical User Interface</b>", "<b>Databricks CLI</b>", or "<b>Databricks REST API</b>".
# MAGIC * "<b>All-Purpose Clusters</b>" are "<b>Persistent</b>". That means, an "<b>All-Purpose Cluster</b>" can be "<b>Terminated</b>", and, "<b>Re-Started</b>" at "<b>Any Point</b>" in "<b>Time</b>".
# MAGIC * "<b>All-Purpose Clusters</b>" are "<b>Suitable</b>" for the "<b>Interactive</b>", and, "<b>Ad-Hoc Analysis Workloads</b>".
# MAGIC * An "<b>All-Purpose Clusters</b>" can be "<b>Shared</b>" among the "<b>Many Users</b>" in a "<b>Databricks Workspace</b>". Therefore, the * "<b>All-Purpose Clusters</b>" are "<b>Good</b>" for "<b>Collaborative Analysis</b>".
# MAGIC * "<b>All-Purpose Clusters</b>" are "<b>Expensive</b>" to "<b>Run</b>" "<b>Compared</b>" to the * "<b>Job Clusters</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Job Cluster"?
# MAGIC * A "<b>Job Cluster</b>" is "<b>Created</b>" when a "<b>Job</b>" "<b>Starts</b>" to "<b>Execute</b>", and, that "<b>Job</b>" has been "<b>Configured</b>" to use a "<b>Job Cluster</b>".
# MAGIC * A "<b>Job Cluster</b>" is "<b>Terminated</b>" at the "<b>End</b>" of a "<b>Job</b>".
# MAGIC <br>A "<b>Job Cluster</b>" "<b>Can Not</b>" be "<b>Re-Started</b>". So, a "<b>Job Cluster</b>" is "<b>No Longer Usable</b>" "<b>Once</b>" the "<b>Job</b>" has been "<b>Completed</b>".
# MAGIC * "<b>Job Clusters</b>" are "<b>Suitable</b>" for the "<b>Automated Workloads</b>", i.e., "<b>Repeated Production Workloads</b>", such as - "<b>Running an ETL Pipeline At a Regular Interval</b>", or, "<b>Running a Machine Learning Workflow At a Regular Interval</b>".
# MAGIC * The "<b>Job Clusters</b>" are "<b>Isolated</b>" just for the "<b>Job</b>" being "<b>Executed</b>".
# MAGIC * "<b>Job Clusters</b>" are "<b>Cheaper</b>" to "<b>Run</b>" "<b>Compared</b>" to the "<b>All-Purpose Clusters</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Cluster Pools"
# MAGIC * Usually, when a "<b>Cluster</b>" is "<b>Created</b>", it takes about "<b>5 to 6 Minutes</b>" to "<b>Spin Up</b>" the "<b>Cluster</b>".
# MAGIC <br>In order to "<b>Speed Up</b>" that "<b>Time</b>", it is possible to "<b>Keep</b>" a "<b>Pool of Resources</b>" "<b>Waiting</b>". That is where the "<b>Cluster Pools</b>" "<b>Comes In</b>".
# MAGIC * The "<b>Cluster Pools</b>" "<b>Gives</b>" the "<b>Users</b>" the "<b>Ability</b>" to "<b>Set Aside</b>" some "<b>Ready-To-Use</b>" "<b>Compute Capacity</b>" so that when an "<b>All-Purpose Cluster</b>" is "<b>Created</b>", it can be "<b>Created Quickly</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Cluster Policy"?
# MAGIC * When "<b>Creating</b>" a "<b>Cluster</b>", there are "<b>Lots of Configuration Options</b>" to "<b>Specify</b>".
# MAGIC * The "<b>Cluster Policy</b>" "<b>Helps</b>" the "<b>Users</b>" to "<b>Pre-Configure</b>" some of the "<b>Configuration Options</b>" so that "<b>Creating</b>" a "<b>Cluster</b>" can become "<b>Simpler</b>".
# MAGIC * The "<b>Cluster Policy</b>" also "<b>Helps</b>" to "<b>Restrict</b>" the "<b>Maximium Size</b>" of the "<b>Cluster</b>" being "<b>Created</b>" to "<b>Keep</b>" the "<b>Cost Under Control</b>".
# MAGIC * "<b>Cluster Policy</b>" is "<b>Only Available</b>" on the "<b>Premium Tier</b>". So, the "<b>Cluster Policy</b>" is "<b>Not Accessible</b>" in the "<b>Standard Tier</b>" of "<b>Workspace</b>".
