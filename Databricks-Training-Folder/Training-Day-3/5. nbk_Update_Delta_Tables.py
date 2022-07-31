# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 3
# MAGIC * Topic: "Update" a "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Update a Delta Table
# MAGIC * It is possible to "Update" the "Column Values" for the "Rows" of a "Delta Table" that "Matches" a "Predicate".
# MAGIC * When "No Predicate" is Provided, the "Column Values" for "All" the "Rows" are "Updated".

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

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path"
# MAGIC %sql
# MAGIC select * from retailer_db.tbl_DeltaCustomerFromDataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC # Using "DeltaTableBuilder API"

# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined in the "Metastore"


# COMMAND ----------

# DBTITLE 1,"Update" Column Values of a "Delta Table", Defined by "Path"

