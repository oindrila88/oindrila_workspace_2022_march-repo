# Databricks notebook source
# MAGIC %md
# MAGIC # Update Data of Existing Delta Tables in Databricks
# MAGIC * Topic: "Update" a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Update Data of a Delta Table
# MAGIC * It is possible to "Update" the "Column Values" for the "Rows" of a "Delta Table" that "Matches" a "Predicate".
# MAGIC * When "No Predicate" is Provided, the "Column Values" for "All" the "Rows" are "Updated".
# MAGIC * When possible, it is best to Provide the "Predicates" on the "Partition Columns" for a "Partitioned Delta Table", because the "Predicates" can Significantly "Speed Up" the "Update Operation".

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "SQL DML Command"

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined in the "Metastore", With "Matching Predicate"
# MAGIC %sql
# MAGIC UPDATE retailer_db.tbl_DeltaCustomerFromDataFrame
# MAGIC SET c_login = 'From Japan'
# MAGIC WHERE TRIM(c_birth_country) = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,Verify the Value of the Column "c_login" for the Matching Predicate Value of Column "c_birth_country" as "JAPAN"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined in the "Metastore", Without "Matching Predicate"
# MAGIC %sql
# MAGIC UPDATE retailer_db.tbl_DeltaCustomerFromDataFrame
# MAGIC SET c_login = NULL

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path", With "Matching Predicate"
# MAGIC %sql
# MAGIC UPDATE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`
# MAGIC SET c_login = 'From Japan'
# MAGIC WHERE TRIM(c_birth_country) = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,Verify the Value of the Column "c_login" for the Matching Predicate Value of Column "c_birth_country" as "JAPAN"
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path", Without "Matching Predicate"
# MAGIC %sql
# MAGIC UPDATE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`
# MAGIC SET c_login = NULL

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "DeltaTableBuilder API"

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined in the "Metastore", With "Matching Predicate"
from delta.tables import *

deltaTableByName = DeltaTable.forName(spark, 'retailer_db.tbl_DeltaCustomerFromDataFrame')

# Declare the "Predicate" by Using a "SQL-Formatted String".
deltaTableByName.update(
  condition = "TRIM(c_birth_country) = 'JAPAN'",
  set = { "c_login": "'From JAPAN'" }
)

# COMMAND ----------

# DBTITLE 1,Verify the Value of the Column "c_login" for the Matching Predicate Value of Column "c_birth_country" as "JAPAN"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined in the "Metastore", Without "Matching Predicate"
from delta.tables import *
from pyspark.sql.functions import *

deltaTableByName = DeltaTable.forName(spark, 'retailer_db.tbl_DeltaCustomerFromDataFrame')

deltaTableByName.update(
    set = { "c_login": lit(None) }
)

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path", With "Matching Predicate"
from delta.tables import *
from pyspark.sql.functions import *

deltaTableByPath = DeltaTable.forPath(spark, '/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath')

# Declare the "Predicate" by Using a "SQL Functions".
deltaTableByPath.update(
  condition = trim(col('c_birth_country')) == 'JAPAN',
  set = { 'c_login': lit('From JAPAN') }
)

# COMMAND ----------

# DBTITLE 1,Verify the Value of the Column "c_login" for the Matching Predicate Value of Column "c_birth_country" as "JAPAN"
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path", Without "Matching Predicate"
from delta.tables import *
from pyspark.sql.functions import *

deltaTableByPath = DeltaTable.forPath(spark, '/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath')

# Declare the "Predicate" by Using a "SQL Functions".
deltaTableByPath.update(
  set = { 'c_login': lit(None) }
)
