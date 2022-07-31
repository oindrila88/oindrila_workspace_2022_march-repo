# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 3
# MAGIC * Topic: "Delete" Data from a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete Data from a Delta Table
# MAGIC * It is possible to "Remove" the "Rows" of a "Delta Table" that "Matches" a "Predicate".
# MAGIC * When "No Predicate" is Provided, "All" the "Rows" are "Removed".
# MAGIC * "delete" "Removes" the Data from the "Latest Version" of the "Delta Table" but "Does Not Remove" the Data from the "Physical Storage" until the "Old Versions" are "Explicitly Vacuumed".
# MAGIC * When possible, it is best to Provide the "Predicates" on the "Partition Columns" for a "Partitioned Delta Table", because the "Predicates" can Significantly "Speed Up" the "Operation".

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "SQL DML Command"

# COMMAND ----------

# DBTITLE 1,"Remove" Rows from a "Delta Table", Defined in the "Metastore", With "Matching Predicate"
# MAGIC %sql
# MAGIC DELETE FROM retailer_db.tbl_DeltaCustomerFromDataFrame
# MAGIC WHERE TRIM(c_birth_country) = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,Verify If the Rows for the Matching Predicate Value of Column "c_birth_country" as "JAPAN" are "Removed"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the Rows of a "Delta Table", Defined in the "Metastore", Without "Matching Predicate"
# MAGIC %sql
# MAGIC DELETE FROM retailer_db.tbl_DeltaCustomerFromDataFrame;

# COMMAND ----------

# DBTITLE 1,"Restore" the Delta Table, Defined in the "Metastore", to the "Version" Before the "Delete" Operation
# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY retailer_db.tbl_DeltaCustomerFromDataFrame;
# MAGIC RESTORE retailer_db.tbl_DeltaCustomerFromDataFrame TO VERSION AS OF 4;

# COMMAND ----------

# DBTITLE 1,"Remove" Rows from a "Delta Table", Defined by "Path", With "Matching Predicate"
# MAGIC %sql
# MAGIC DELETE FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`
# MAGIC WHERE TRIM(c_birth_country) = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,Verify If the Rows for the Matching Predicate Value of Column "c_birth_country" as "JAPAN" are "Removed"
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the Rows of a "Delta Table", Defined by "Path", Without "Matching Predicate"
# MAGIC %sql
# MAGIC DELETE FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`;

# COMMAND ----------

# DBTITLE 1,"Restore" the Delta Table, Defined by "Path", to the "Version" Before the "Delete" Operation
# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`;
# MAGIC RESTORE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` TO VERSION AS OF 4;

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "DeltaTableBuilder API"

# COMMAND ----------

# DBTITLE 1,"Remove" Rows from a "Delta Table", Defined in the "Metastore", With "Matching Predicate"
from delta.tables import *

deltaTableByName = DeltaTable.forName(spark, 'retailer_db.tbl_DeltaCustomerFromDataFrame')

# Declare the "Predicate" by Using a "SQL-Formatted String".
deltaTableByName.delete("c_birth_country == 'JAPAN'")

# COMMAND ----------

# DBTITLE 1,Verify If the Rows for the Matching Predicate Value of Column "c_birth_country" as "JAPAN" are "Removed"
# MAGIC %sql
# MAGIC SELECT * FROM retailer_db.tbl_DeltaCustomerFromDataFrame WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the Rows of a "Delta Table", Defined in the "Metastore", Without "Matching Predicate"
from delta.tables import *

deltaTableByName = DeltaTable.forName(spark, 'retailer_db.tbl_DeltaCustomerFromDataFrame')

deltaTableByName.delete()

# COMMAND ----------

# DBTITLE 1,"Restore" the Delta Table, Defined in the "Metastore", to the "Version" Before the "Delete" Operation
# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY retailer_db.tbl_DeltaCustomerFromDataFrame;
# MAGIC -- RESTORE retailer_db.tbl_DeltaCustomerFromDataFrame TO VERSION AS OF 7;

# COMMAND ----------

# DBTITLE 1,"Remove" Rows from a "Delta Table", Defined by "Path", With "Matching Predicate"
from delta.tables import *
from pyspark.sql.functions import *

deltaTableByPath = DeltaTable.forPath(spark, '/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath')

# Declare the "Predicate" by Using a "SQL Functions".
deltaTableByPath.delete(trim(col('c_birth_country')) == 'JAPAN')

# COMMAND ----------

# DBTITLE 1,Verify If the Rows for the Matching Predicate Value of Column "c_birth_country" as "JAPAN" are "Removed"
# MAGIC %sql
# MAGIC SELECT * FROM delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` WHERE c_birth_country = 'JAPAN';

# COMMAND ----------

# DBTITLE 1,"Remove" "All" the Rows of a "Delta Table", Defined by "Path", Without "Matching Predicate"
from delta.tables import *

deltaTableByPath = DeltaTable.forPath(spark, '/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath')

deltaTableByPath.delete()

# COMMAND ----------

# DBTITLE 1,"Restore" the Delta Table, Defined by "Path", to the "Version" Before the "Delete" Operation
# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath`;
# MAGIC -- RESTORE delta. `/tmp/tables/delta/tbl_DeltaCustomerFromDataFrameWithPath` TO VERSION AS OF 7;
