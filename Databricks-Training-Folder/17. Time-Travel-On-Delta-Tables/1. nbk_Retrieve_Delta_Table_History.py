# Databricks notebook source
# MAGIC %md
# MAGIC # View History Information of a Delta Table in Databricks
# MAGIC * Topic: How to Retrieve the "History" of a "Delta Table"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Retrieve Delta Table History
# MAGIC * It is possible to "Retrieve" the "Information" on the "Operations", "User", "Timestamp", and so on for "Each Write" to a "Delta Table" by running the "History" Command.
# MAGIC * The "Operations" are "Returned" in "Reverse Chronological Order". By default, the "Table History" is "Retained" for "30 days".

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY training.customers;

# COMMAND ----------

# DBTITLE 1,Retrieve the "History" for the "Last Operation Only" of a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY training.customers LIMIT 1;

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined by "Path", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`

# COMMAND ----------

# DBTITLE 1,Retrieve the "History" for the "Last Operation Only" of a "Delta Table", Defined by "Path", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath' LIMIT 1

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined in the "Metastore", Using Python
from delta import *

tblDelta = DeltaTable.forName(spark, "training.customers")
df_FullHistory = tblDelta.history()
display(df_FullHistory)

# COMMAND ----------

# DBTITLE 1,Retrieve the "History" for the "Last Operation Only" of a "Delta Table", Defined in the "Metastore", Using Python
from delta import *

tblDelta = DeltaTable.forName(spark, "training.customers")
df_FullHistory = tblDelta.history(1)
display(df_FullHistory)

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined by "Path", Using Python
from delta import *

tblDelta = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")
df_FullHistory = tblDelta.history()
display(df_FullHistory)

# COMMAND ----------

# DBTITLE 1,Retrieve the "History" for the "Last Operation Only" of a "Delta Table", Defined by "Path", Using Python
from delta import *

tblDelta = DeltaTable.forPath(spark, "/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath")
df_LastOperationHistory = tblDelta.history(1)
display(df_LastOperationHistory)
