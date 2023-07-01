# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks User Interface
# MAGIC * Topic: Introduction to "Azure Databricks User Interface"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Using" the "Sidebar"
# MAGIC * The "<b>First Menu Item</b>" on the "<b>Sidebar</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Choose</b>" the "<b>Persona</b>".
# MAGIC <br>Currently, there are "<b>Three Personas</b>", namely -
# MAGIC   * <b>1</b>. <b>Data Science & Engineering</b>
# MAGIC   * <b>2</b>. <b>Machine Learning</b>
# MAGIC   * <b>3</b>. <b>Databricks SQL</b> - This is "<b>Mainly Targetted</b>" at "<b>Data Analysts</b>", but, is "<b>Only Available</b>" on "<b>Premium Tier</b>". This "<b>Persona</b>" will "<b>Not</b>" be "<b>Seen</b>" on the "<b>Databricks Workspace</b>" of "<b>Standard Tier</b>".
# MAGIC <br><img src = '/files/tables/images/sidebar_personas_image.jpg'>
# MAGIC * "<b>Depending</b>" on the "<b>Choice</b>" of "<b>Persona</b>", the "<b>Menu Items</b>" "<b>Change</b>" on the "<b>Sidebar</b>".
# MAGIC * If the "<b>Data Science & Engineering</b>" "<b>Persona</b>" is "<b>Chosen</b>", the following "<b>Menu Items</b>" are "<b>Displayed</b>" -
# MAGIC   * <b>1</b>. <b>New</b>
# MAGIC   * <b>2</b>. <b>Workspace</b>
# MAGIC   * <b>3</b>. <b>Repos</b>
# MAGIC   * <b>4</b>. <b>Recents</b>
# MAGIC   * <b>5</b>. <b>Data</b>
# MAGIC   * <b>6</b>. <b>Compute</b>
# MAGIC   * <b>7</b>. <b>Workflows</b>
# MAGIC <br><img src = '/files/tables/images/sidebar_dse_persona_image.jpg'>
# MAGIC * If the "<b>Machine Learning</b>" "<b>Persona</b>" is "<b>Chosen</b>", the following "<b>Menu Items</b>" are "<b>Displayed</b>", which are "<b>Specific</b>" for "<b>Machine Learning Engineers</b>" -
# MAGIC   * <b>1</b>. <b>New</b>
# MAGIC   * <b>2</b>. <b>Workspace</b>
# MAGIC   * <b>3</b>. <b>Repos</b>
# MAGIC   * <b>4</b>. <b>Recents</b>
# MAGIC   * <b>5</b>. <b>Data</b>
# MAGIC   * <b>6</b>. <b>Compute</b>
# MAGIC   * <b>7</b>. <b>Workflows</b>
# MAGIC   * <b>8</b>. <b>Experiments</b>
# MAGIC   * <b>9</b>. <b>Feature Store</b>
# MAGIC   * <b>10</b>. <b>Models</b>
# MAGIC <br><img src = '/files/tables/images/sidebar_ml_persona_image.jpg'>
# MAGIC * If the "<b>Databricks SQL</b>" "<b>Persona</b>" is "<b>Chosen</b>", the following "<b>Menu Items</b>" are "<b>Displayed</b>", which are "<b>Specific</b>" for "<b>Databricks SQL</b>" -
# MAGIC   * <b>1</b>. <b>New</b>
# MAGIC   * <b>2</b>. <b>SQL Editor</b>
# MAGIC   * <b>3</b>. <b>Workspace</b>
# MAGIC   * <b>4</b>. <b>Queries</b>
# MAGIC   * <b>5</b>. <b>Dashboards</b>
# MAGIC   * <b>6</b>. <b>Alerts</b>
# MAGIC   * <b>7</b>. <b>Data</b>
# MAGIC   * <b>8</b>. <b>SQL Warehouses</b>
# MAGIC   * <b>9</b>. <b>Query History</b>
# MAGIC <br><img src = '/files/tables/images/sidebar_sql_persona_image.jpg'>
# MAGIC * The "<b>New</b>" "<b>Menu Item</b>" of the "<b>Data Science & Engineering</b>" "<b>Persona</b>" on the "<b>Sidebar</b>" "<b>Gives</b>" the "<b>Shortcut</b>" to "<b>Create</b>" some of the "<b>Databricks Objects</b>", such as - "<b>Notebooks</b>", "<b>Clusters</b>", "<b>Jobs</b>" etc.
# MAGIC <br><img src = '/files/tables/images/sidebar_new_menu_item_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Workspace"?
# MAGIC * "<b>Workspace</b>" is basically a "<b>Container</b>" for "<b>Holding</b>" a "<b>Set</b>" of "<b>Folders</b>", "<b>Libraries</b>", and, "<b>Files</b>".
# MAGIC * By default, "<b>Each User</b>" has their "<b>Own Workspace</b>".
# MAGIC * <b>Shared Workspace</b> - The "<b>Shared Workspace</b>" can be used to "<b>Share</b>" the "<b>Assets</b>" among the "<b>Other Users</b>" in a "<b>Workspace</b>".
# MAGIC <br><img src = '/files/tables/images/workspace_image.jpg'>
# MAGIC * By "<b>Clicking</b>" on the "<b>Dropdown</b>" "<b>Beside</b>" the "<b>User Name</b>" in a "<b>Workspace</b>", it is possible to "<b>Perform</b>" the following things -
# MAGIC <br><b>1</b>. <b>Create</b> - It is possible to "<b>Create</b>" the "<b>Notebooks</b>", "<b>Libraries</b>", "<b>Folders</b>", "<b>MLflow Experiments</b>".
# MAGIC <br><img src = '/files/tables/images/workspace_user_create_image.jpg'>
# MAGIC <br><b>2</b>. <b>Import</b> - It is possible to "<b>Import</b>" the "<b>Databricks Assets</b>" of the "<b>Type</b>" as "<b>Notebook</b>", "<b>Library</b>", "<b>Folder</b>", "<b>MLflow Experiment</b>" from "<b>Elsewhere</b>".
# MAGIC <br><img src = '/files/tables/images/workspace_import_image.jpg'>
# MAGIC <br><b>3</b>. <b>Export</b> - It is possible to "<b>Export</b>" the "<b>Databricks Assets</b>" of the "<b>Type</b>" as "<b>Notebooks</b>", or, "<b>Folders</b>" into a "<b>Databricks Format File</b>", i.e., "<b>.dbc</b>", or, "<b>Source Specific File</b>". i.e., "<b>.py</b>", or, "<b>.sql</b>", or, "<b>.scala</b>".
# MAGIC <br><img src = '/files/tables/images/workspace_export_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the "Repos" "Sidebar Menu Item"
# MAGIC * The "<b>Repos</b>" "<b>Sidebar Menu Item</b>" "<b>Gives</b>" a "<b>Visual GIT Client</b>" in "<b>Azure Databricks</b>".
# MAGIC * The "<b>Repos</b>" "<b>Sidebar Menu Item</b>" "<b>Supports</b>" the "<b>Standard Operations</b>", such as - "<b>commit</b>", "<b>push</b>", "<b>pull</b>" etc. from a "<b>GIT Repository</b>".
# MAGIC * "<b>Azure Databricks</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Integrate</b>" a "<b>Azure Databricks Workspace</b>" with the "<b>GIT Repositories</b>", "<b>Offerred By</b>" "<b>Most</b>" of the "<b>GIT Providers</b>", such as - "<b>GitHub</b>", "<b>BitBucket</b>", "<b>Azure DevOps Services</b>" etc.
# MAGIC <br><img src = '/files/tables/images/repos_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the "Data" "Sidebar Menu Item"
# MAGIC * The "<b>Data</b>" "<b>Sidebar Menu Item</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Interact</b>" with "<b>Any</b>" of the "<b>Tables</b>", or, "<b>Views</b>" that have been "<b>Created</b>".
# MAGIC <br>The "<b>Data</b>" "<b>Sidebar Menu Item</b>" also "<b>Allows</b>" to "<b>Create</b>" "<b>New Tables</b>".
# MAGIC * As the "<b>Message</b>" "<b>States</b>" in the "<b>Below Image</b>", the "<b>Users</b>" need to have a "<b>Running Cluster</b>" in the "<b>Workspace</b>" in order to "<b>Access</b>", or, "<b>Use</b>" "<b>Any</b>" of the "<b>Tables</b>", or, "<b>Views</b>" that have been "<b>Created</b>".
# MAGIC <br><img src = '/files/tables/images/data_menu_item_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the "Compute" "Sidebar Menu Item"
# MAGIC * The "<b>Compute</b>" "<b>Sidebar Menu Item</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Create</b>" the "<b>Clusters</b>", and, the "<b>Cluster Pools</b>".
# MAGIC * It is possible to "<b>Either Create</b>" an "<b>All-Purpose Cluster</b>", or, a "<b>Job Cluster</b>".
# MAGIC <br><img src = '/files/tables/images/compute_menu_item_image.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the "Workflows" "Sidebar Menu Item"
# MAGIC * The "<b>Workflows</b>" "<b>Sidebar Menu Item</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Create</b>" the "<b>Databricks Jobs</b>", and, "<b>Monitor</b>" the "<b>Progress</b>" of the "<b>Created Databricks Jobs</b>".
# MAGIC <br><img src = '/files/tables/images/workflows_menu_item_image.jpg'>
# MAGIC * "<b>Databricks Jobs</b>" basically "<b>Allow</b>" the "<b>Users</b>" to "<b>Schedule</b>" the "<b>Notebooks Periodically</b>" via a "<b>Scheduling System</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Management Capabilities" in "Databricks"
# MAGIC * The "<b>Menu</b>" at the "<b>Top Right Corner</b>" "<b>Gives</b>" some of the "<b>Management Capabilities</b>" "<b>Required</b>" for the "<b>Azure Databricks Workspace</b>", and, its "<b>Users</b>".
# MAGIC <br><img src = '/files/tables/images/management_menu_item_image.jpg'>
# MAGIC * <b>1</b>. <b>User Settings</b> - The <b>User Settings</b> "<b>Menu Item</b>" "<b>Allows</b>" the "<b>Users</b>" to "<b>Perform</b>" the following things -
# MAGIC   * "<b>Enable</b>" the "<b>GIT Integration</b>" for an "<b>Azure Databricks Workspace</b>".
# MAGIC   * "<b>Generate</b>" an "<b>Access Token</b>" to be used by "<b>Another Application</b>", for example - "<b>PowerBI</b>" etc.
# MAGIC <br><img src = '/files/tables/images/user_settings_menu_item_image.jpg'>
# MAGIC * <b>2</b>. <b>Admin Console</b> - The <b>Admin Console</b> "<b>Menu Item</b>" "<b>Gives</b>" the "<b>Users</b>" the "<b>Ability</b>" to "<b>Manage</b>" the following things -
# MAGIC   * the "<b>Users</b>"
# MAGIC   * the "<b>Groups</b>"
# MAGIC   * the "<b>Workspace Settings</b>"
# MAGIC <br><img src = '/files/tables/images/admin_console_menu_item_image.jpg'>
