# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Basics
# MAGIC * Topic: Introduction to the "Clusters" in Databricks
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Cluster" in "Databricks"
# MAGIC * A "Databricks Cluster" is a "Set" of "Computation Resources" and "Configurations" on which the "<b>Data Engineering</b>", "<b>Data Science</b>", and "<b>Data Analytics Workloads</b>" are "Run", such as "<b>Production ETL Pipelines</b>", "<b>Streaming Analytics</b>", "<b>Ad-Hoc Analytics</b>", and "<b>Machine Learning</b>". These "Workloads" are "Run" as a "Set of Commands" in a "Notebook" or as an "Automated Job".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Different Types" of "Clusters" in "Databricks"
# MAGIC * There are "Two" types of "Databricks Clusters" -
# MAGIC * A. <b>All-Purpose Clusters</b>: An "<b>All-Purpose Cluster</b>" can be "Created" using the "UI", "CLI", or "REST API".<br>It is possible to "Manually Terminate" and "Restart" an "<b>All-Purpose Cluster</b>".<br>"Multiple Users" can "Share" such "Clusters" to do "<b>Collaborative Interactive Analysis</b>" using "<b>Interactive Notebooks</b>".
# MAGIC * B. <b>Job Clusters</b>: The "<b>Databricks Job Scheduler</b>" "Creates" a "<b>Job Cluster</b>" when a "Job" is "Run" on a "<b>New Job Cluster</b>" and "Terminates" the "Cluster" when the "Job" is "Complete". It is "Not Possible" to "Restart" a "Job Cluster".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Different" Types of "Cluster Modes"
# MAGIC * "<b>Azure Databricks</b>" supports the following "<b>Three</b>" Types of "<b>Cluster Modes</b>" -
# MAGIC * A. <b>Standard</b> or <b>No Isolation Shared</b>: "<b>Standard</b>" Mode Clusters, or, the "<b>No Isolation Shared</b>" Clusters can be "<b>Shared</b>" by "<b>Multiple Users</b>", with "<b>No Isolation</b>" between "<b>Users</b>".<br>A "<b>Standard</b>" Mode Cluster, or, the "<b>No Isolation Shared</b>" Cluster "Requires" "At Least One" "Spark Worker Node" in addition to the "Driver Node" to "Execute" the "Spark Jobs".<br>A "<b>Standard</b>" Mode Cluster, or, the "<b>No Isolation Shared</b>" Cluster is recommended for "<b>Single Users</b>" only.<br>"<b>Standard</b>" Mode Clusters, or, the "<b>No Isolation Shared</b>" Clusters can "Run" the "<b>Workloads</b>" that are "Developed" in "<b>Python</b>", "<b>SQL</b>", "<b>R</b>", and "<b>Scala</b>".<br>"<b>Standard</b>" Mode Clusters, or, the "<b>No Isolation Shared</b>" Clusters "<b>Terminate Automatically</b>" "<b>After 120 Minutes</b>" by default.
# MAGIC * B. <b>High Concurrency with Tables ACLs</b> or <b>Shared Access Mode Cluster</b>: A "<b>High Concurrency</b>" Cluster, or, "<b>Shared Access</b>" Mode Cluster is a "Managed Cloud Resource".<br>The "Key Benefits" of the "<b>High Concurrency</b>" Clusters, or, "<b>Shared Access</b>" Mode Clusters are that these provide "<b>Fine-Grained Sharing</b>" for "<b>Maximum Resource Utilization</b>" and "<b>Minimum Query Latencies</b>".<br>"<b>High Concurrency</b>" Cluster, or, "<b>Shared Access</b>" Mode Cluster can "Run" the "<b>Workloads</b>" that are "Developed" in "<b>SQL</b>", "<b>Python</b>", and "<b>R</b>".<br>The "<b>Performance</b>" and "<b>Security</b>" of "<b>High Concurrency</b>" Cluster, or, "<b>Shared Access</b>" Mode Cluster is provided by "<b>Running</b>" the "<b>User Code</b>" in "<b>Separate Processes</b>", which is "<b>Not Possible</b>" in "<b>Scala</b>".<br>Only "<b>High Concurrency</b>" Cluster, or, "<b>Shared Access</b>" Mode Cluster "Supports" the "<b>Table Access Control</b>".<br>"<b>High Concurrency</b>" Cluster, or, "<b>Shared Access</b>" Mode Clusters "<b>Do Not</b>" "<b>Terminate Automatically</b>" by default.
# MAGIC * C. <b>Single Node</b>: A "<b>Single Node</b>" Cluster has "<b>No Workers</b>" and "Runs" the "<b>Spark Jobs</b>" on the "<b>Driver Node</b>".<br>A "<b>Single Node</b>" Cluster "Supports" the "<b>Spark Jobs</b>" and "<b>All</b>" the "<b>Spark Data Sources</b>", including "<b>Delta Lake</b>".
