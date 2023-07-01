# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks Architecture
# MAGIC * Topic: Introduction to "Azure Databricks Architecture"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Architecture" of "Azure Databricks"
# MAGIC * "<b>Azure Databricks</b>" is a "<b>Managed Service</b>" on the "<b>Cloud</b>". As an "<b>Azure Databricks Developer</b>", it is good to "<b>Understand</b>" the "<b>Architecture</b>" so that the "<b>Developers</b>" "<b>Know</b>" "<b>Where</b>" the "<b>Data</b>" is "<b>Stored</b>", and, "<b>Where</b>" the "<b>Cluster</b>" is "<b>Located</b>".
# MAGIC * The "<b>Azure Databricks Architecture</b>" is basically "<b>Split</b>" into "<b>Two Parts</b>", namely -
# MAGIC   * <b>1</b>. <b>Control Plane</b> - The "<b>Control Plane</b>" is "<b>Located</b>" in the "<b>Databricks' Own Subscription</b>".
# MAGIC   <br>The "<b>Control Plane</b>" "<b>Contans</b>" the "<b>Databricks UX</b>", and, the "<b>Cluster Manager</b>".
# MAGIC   <br>The "<b>Control Plane</b>" is also the "<b>Home</b>" to the "<b>Databricks File System</b>", i.e., "<b>DBFS</b>".
# MAGIC   <br>The "<b>Control Plane</b>" also "<b>Contans</b>" the "<b>Metadata</b>" about the "<b>Mounted Files</b>" in the "<b>Cluster</b>".
# MAGIC   <br><img src = '/files/tables/images/control_plane_image.jpg'>
# MAGIC   * <b>2</b>. <b>Data Plane</b> - The "<b>Data Plane</b>" is "<b>Located</b>" in the "<b>Customer Subscription</b>".
# MAGIC   <br>When a "<b>Databricks Service</b>" is "<b>Created</b>" in "<b>Azure</b>", the following "<b>Four Resources</b>" get "<b>Created</b>" in the "<b>Azure Subscription</b>" -
# MAGIC   <br>1</b>. <b>Virtual Network</b>
# MAGIC   <br>2</b>. <b>Network Security Group</b> for the <b>Virtual Network</b>
# MAGIC   <br>3</b>. <b>Azure Blob Storage</b> for the "<b>Default Storage</b>"
# MAGIC   <br>4</b>. <b>Databricks Workspace</b>
# MAGIC   <br><img src = '/files/tables/images/data_plane_image.jpg'>
# MAGIC   <br>It can be seen from the "<b>Below Image</b>" of "<b>Azure Portal</b>" that the "<b>Third Resource</b>" is the "<b>Azure Databricks Workspace</b>" that has been "<b>Created</b>" in the "<b>User-Provided</b>" "<b>Resource Group Name</b>", i.e., "<b>RG-Databricks</b>".
# MAGIC   <br>Whereas, "<b>All</b>" of the "<b>Other Resources</b>" are "<b>Created</b>" by the "<b>Azure Databricks</b>" itself in the "<b>Resource Group</b>", which is also "<b>Created</b>" by the "<b>Azure Databricks</b>" "<b>Behind the Scene</b>".
# MAGIC   <br>"<b>All</b>" of the "<b>Five Resources</b>" are "<b>Present</b>" in the "<b>Customer Subscription</b>", i.e., in the "<b>Pay-As-You-Go</b>" "<b>Azure Subscription</b>".
# MAGIC   <br><img src = '/files/tables/images/azure_databricks_resources_in_azure_portal_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What Happens When "Databricks Clusters" are "Created"?
# MAGIC * The "<b>Databricks Users</b>", such as - the "<b>Data Engineers</b>", "<b>Data Scientists</b>", and, "<b>Data Analysts</b>" will use the "<b>Azure Active Directory Single Sign On</b>" to "<b>Access</b>" the "<b>Azure Databricks Service</b>".
# MAGIC * When a "<b>User</b>" "<b>Requests</b>" for a "<b>Cluster</b>" to be "<b>Created</b>" to the "<b>Azure Databricks Service</b>", the "<b>Databricks Cluster Manager</b>" will "<b>Create</b>" the required "<b>Virtual Machines</b>" in the "<b>Customer Subscription</b>", i.e., in the "<b>Azure Subscription</b>" via the "<b>Azure Resource Manager</b>".
# MAGIC * So, "<b>None</b>" of the "<b>Customer Data</b>" "<b>Leaves</b>" the "<b>Customer Subscription</b>", i.e., the "<b>Azure Subscription</b>".
# MAGIC <br>The "<b>Temporary Outputs</b>", such as - "<b>Running</b>" the "<b>display ()</b>" "<b>Command</b>", or, the "<b>Data</b>" for the "<b>Managed Tables</b>" are "<b>Stored</b>" within the "<b>Azure Blob Storage</b>".
# MAGIC <br>The "<b>Processing</b>" on the "<b>Customer Data</b>" also "<b>Happens</b>" within the "<b>Virtual Network</b>" in the "<b>Customer Subscription</b>", i.e., the "<b>Azure Subscription</b>".
# MAGIC <br><img src = '/files/tables/images/cluster_creation_image.jpg'>
# MAGIC * The "<b>Azure Blob Storage</b>" is the "<b>Default Storage</b>", which is also called as the "<b>DBFS Root</b>".
# MAGIC <br>The "<b>Azure Blob Storage</b>" is "<b>Not Recommended</b>" as a "<b>Permanent Data Storage</b>".
# MAGIC <br>For the "<b>Permanent Data Storage</b>", it is possible to "<b>Mount</b>" the "<b>Other Storage Accounts</b>" from the "<b>Customer Subscription</b>", i.e., the "<b>Azure Subscription</b>"
