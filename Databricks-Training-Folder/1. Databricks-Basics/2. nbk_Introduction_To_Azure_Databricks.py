# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Basics
# MAGIC * Topic: What is "Azure Databricks"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Working" with "Apache Spark" "Requires" "Effort" on "Cloud"
# MAGIC * "<b>Apache Spark</b>" is a "<b>Fast Execution Engine</b>" with an "<b>Easy to Use</b>" "<b>Set</b>" of "<b>Higher Level APIs</b>".
# MAGIC * But, in order to "<b>Work</b>" with "<b>Apache Spark</b>" on "<b>Cloud</b>", the "<b>Developers</b>" need to "<b>Perform</b>" the following things -
# MAGIC   * "<b>Set Up</b>" the "<b>Clusters</b>".
# MAGIC   * "<b>Manage</b>" the "<b>Security</b>".
# MAGIC   * "<b>Use</b>" the "<b>Third-Party Products</b>" to "<b>Write</b>" the "<b>Programs</b>".
# MAGIC * That's where "<b>Databricks</b>" comes in.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Azure Databricks"?
# MAGIC * "<b>Azure Databricks</b>" is a "<b>Spark-Based Unified Data Analytics</b>" "<b>Platform-As-A-Service Offering</b>" ("<b>PaaS</b>") that is "<b>Optimized</b>" for the "<b>Microsoft Cloud Platform</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Why" "Databricks" is "Used"?
# MAGIC * "<b>Databricks</b>" is a "<b>Company</b>", which is "<b>Created By</b>" the "<b>Founders</b>" of the "<b>Apache Spark</b>" to "<b>Make It Easier to Work with Apache Spark</b>" on the "<b>Cloud</b>".
# MAGIC * <b>1</b>. <b>Clusters</b> - In order for "<b>Apache Spark</b>" to "<b>Perform</b>" the "<b>Distributed Computing</b>", the "<b>Clusters</b>" need to be "<b>Spin Up</b>", and, the "<b>Softwares</b>" need to be "<b>Installed</b>".
# MAGIC <br>"<b>Databricks</b>" gives the "<b>Ability</b>" to "<b>Spin Up</b>" the "<b>Clusters</b>" with a "<b>Few Clicks</b>".
# MAGIC   * The "<b>Suitable Runtime</b>" of the "<b>Cluster</b>" can be "<b>Chosen</b>", which will be "<b>Useful</b>" for the "<b>Specific Need</b>". Example - A "<b>Runtime</b>" can be "<b>Chosen</b>" with "<b>ML Libraries</b>", "<b>Support</b>" for "<b>GPU</b>" etc.
# MAGIC   * It is possible to "<b>Choose</b>" from the following "<b>Different Types</b>" of "<b>Clusters</b>" -
# MAGIC   <br>"<b>General Purpose</b>"
# MAGIC   <br>"<b>Memory Optimized</b>"
# MAGIC   <br>"<b>Compute Optimized</b>"
# MAGIC   <br>"<b>GPU Enabled</b>"
# MAGIC * <b>2</b>. <b>Workspace / Notebook</b> - "<b>Databricks</b>" "<b>Provides</b>" a "<b>Jupyter-Notebook Styled IDE</b>" with "<b>Additional Capabilities</b>" to "<b>Create</b>" the "<b>Application</b>", "<b>Collaborate</b>" with the "<b>Other Colleagues</b>" working in the "<b>Project</b>", and, also "<b>Integrate</b>" with "<b>Configuration Management Tools</b>", such as - "<b>GIT</b>".
# MAGIC * <b>3</b>. <b>Administration Controls</b> - "<b>Databricks</b>" "<b>Provides</b>" the "<b>Administration Controls</b>" that can be used to "<b>Restrict</b>", or, "<b>Provide</b>" the "<b>Access</b>" to the "<b>Users</b>", "<b>Workspace</b>", "<b>Clusters</b>" etc.
# MAGIC * <b>4</b>. <b>Optimized Spark (5 Times Faster)</b> - "<b>Databricks</b>" "<b>Provides</b>" the "<b>Runtime</b>", which is "<b>Highly Optimized</b>" for the "<b>Databricks Platform</b>", and, known to be "<b>Five Times Faster</b>" than the "<b>Vanilla Apache Spark</b>".
# MAGIC * <b>5</b>. <b>Databases / Tables</b> - With the "<b>Use</b>" of the "<b>Hive Metastore</b>", "<b>Databricks</b>" "<b>Provides</b>" the "<b>Ability</b>" to "<b>Create</b>" the "<b>Databases</b>", or, "<b>Tables</b>".
# MAGIC * <b>6</b>. <b>Delta Lake</b> - In order to "<b>Provide</b>" the "<b>ACID Transaction Capability</b>", "<b>Databricks</b>" comes with the "<b>Open-Source Project</b>", called, the "<b>Delta Lake</b>".
# MAGIC * <b>7</b>. <b>SQL Analytics</b> - The "<b>Recent Addition</b>" to the "<b>Databricks</b>" is the "<b>SQL Analytics</b>", which "<b>Provides</b>" the "<b>Data Analysts</b>" a "<b>SQL-Based Analytical Environment</b>".
# MAGIC <br>This "<b>Allows</b>" the "<b>Data Analysts</b>" to "<b>Explore</b>" the "<b>Data</b>", "<b>Create</b>" a "<b>Dashboard</b>", "<b>Schedule</b>" a "<b>Regular Refresh</b>" of the "<b>Dashboard</b>" etc.
# MAGIC * <b>8</b>. <b>ML Flow</b> - The "<b>Managed ML Flow</b>" on "<b>Databricks</b>" "<b>Allows</b>" to "<b>Manage</b>" the "<b>Machine Learning Lifecycle</b>", which "<b>Includes</b>" the following -
# MAGIC   * "<b>Experimentation</b>"
# MAGIC   * "<b>Deployment Model</b>"
# MAGIC   * "<b>Registry</b>" etc.

# COMMAND ----------

# MAGIC %md
# MAGIC # "Azure Integration" of "Databricks"
# MAGIC * Altough the "<b>Databricks Cloud Platform</b>" is "<b>Available</b>" on "<b>All</b>" of the "<b>Three Major Cloud Platforms</b>", i.e., "<b>Microsoft Azure</b>", "<b>Amazon Web Service</b>", and, "<b>Google Cloud</b>", "<b>Azure's Integration</b>" is "<b>Deeper</b>" than the "<b>Other Cloud Platforms</b>".
# MAGIC * <b>1</b>. <b>Unified Azure Portal, and, Unified Billing</b> - "<b>Databricks</b>" is a "<b>First-Party Service</b>" on "<b>Microsoft Azure</b>". That means that on "<b>Microsoft Azure</b>", the "<b>Users</b>" will "<b>Directly Buy</b>" the "<b>Databricks</b>" from "<b>Microsoft</b>", and, "<b>All Support Requests</b>" are "<b>Handled</b>" by "<b>Microsoft</b>".
# MAGIC <br>As a result, a "<b>Unified Azure Portal</b>" is "<b>Provided</b>" for "<b>Databricks</b>", and, a "<b>Single Unified Bill</b>" is "<b>Provided</b>" for "<b>All Azure Services</b>", "<b>Including</b>" the "<b>Databricks</b>".
# MAGIC * <b>2</b>. <b>Azure Active Directory</b> - "<b>Azure Databricks</b>" "<b>Leverages</b>" the "<b>Azure Security</b>", and, "<b>Seamlessly Integrates</b>" with "<b>Azure Active Directory</b>", and, "<b>Single Sign Ons</b>".
# MAGIC * <b>3</b>. <b>Azure Data Services</b> - "<b>Azure Databricks</b>" "<b>Provides</b>" "<b>Seamless Integrations</b>", and, "<b>High-Speed Connectors</b>" between "<b>Various Azure Data Services</b>", such as - "<b>Azure Data Lake</b>", "<b>Azure Blob Storage</b>", "<b>Azure Cosmos DB</b>", "<b>Azure SQL Database</b>", and, "<b>Azure Synapse</b>".
# MAGIC * <b>4</b>. <b>Azure Messaging Services</b> - "<b>Azure Databricks</b>" "<b>Provides</b>" "<b>Seamless Integrations</b>" between "<b>Various Azure Messaging Services</b>", such as - "<b>Azure IoT Hub</b>", and, "<b>Azure Event Hub</b>".
# MAGIC * <b>5</b>. <b>Power BI</b> - "<b>Azure Databricks</b>" "<b>Provides</b>" "<b>Seamless Integrations</b>" with the "<b>Power BI</b>".
# MAGIC * <b>6</b>. <b>Azure ML</b> - "<b>Azure Databricks</b>" "<b>Provides</b>" "<b>Seamless Integrations</b>" with the "<b>Azure ML</b>".
# MAGIC * <b>7</b>. <b>Azure Data Factory</b> - It is possible to "<b>Seamlessly Run</b>" the "<b>Azure Databricks Notebooks</b>" from "<b>Azure Data Factory</b>", and, "<b>Integrate</b>" with the "<b>Rest</b>" of the "<b>Data Workflow</b>" in the "<b>Data Project</b>".
# MAGIC * <b>8</b>. <b>Azure DevOps</b> - "<b>Azure Databricks</b>" can "<b>Connect</b>" with "<b>Azure DevOps</b>" to "<b>Enable</b>" the "<b>Continuous Integration</b>", and, "<b>Continuous Deployment</b>".
