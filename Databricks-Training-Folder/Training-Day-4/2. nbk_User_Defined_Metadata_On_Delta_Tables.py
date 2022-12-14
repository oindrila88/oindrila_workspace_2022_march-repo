# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: How to Use "User-Defined Metadata" on the "Delta Tables"?
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Set" the "User-Defined Commit Metadata"
# MAGIC * It is possible to "Specify" the "User-Defined Strings" as "Metadata" in "Commits" Made by the Operations, "Using" the following Options -
# MAGIC * A. "DataFrameWriter" Option "<b>userMetadata</b>", or,
# MAGIC * B. "SparkSession Configuration" "<b>spark.databricks.delta.commitInfo.userMetadata</b>".
# MAGIC * If "Both" of the "Options" have been "Specified", then the "DataFrameWriter" Option "<b>userMetadata</b>" "Takes Preference".
# MAGIC * The "User-Defined Metadata" is "Readable" in the "History" Operation.

# COMMAND ----------

# DBTITLE 1,"Set" the "SparkSession Configuration" "spark.databricks.delta.commitInfo.userMetadata" to "True"
spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", True)

# COMMAND ----------

# DBTITLE 1,"Load" the "Data" into the "Delta Table" "retailer_db.tbl_DeltaCustomerWithUserDefinedMetadata" with "User-Defined Metadata"
ddl = """c_customer_sk long comment \"This is the Primary Key\",
         c_customer_id string,
         c_current_cdemo_sk long,
         c_current_hdemo_sk long,
         c_current_addr_sk long,
         c_first_shipto_date_sk long,
         c_first_sales_date_sk long,
         c_salutation string,
         c_first_name string,
         c_last_name string,
         c_preferred_cust_flag string,
         c_birth_day int,
         c_birth_month int,
         c_birth_year int,
         c_birth_country string,
         c_login string,
         c_email_address string,
         c_last_review_date long"""

df_ReadCustomerFileUsingCsv = spark.read\
                                   .option("header", "true")\
                                   .schema(ddl)\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

df_ReadCustomerFileUsingCsv.write.format("delta")\
                                 .mode("append")\
                                 .option("userMetadata", "example-of-table-creation-with-user-defined-metadata")\
                                 .option("path", "/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/user-defined-metadata-on-delta-table")\
                                 .saveAsTable("retailer_db.tbl_DeltaCustomerWithUserDefinedMetadata")

# COMMAND ----------

# DBTITLE 1,"Drop" the Delta Table "retailer_db.tbl_DeltaCustomerWithUserDefinedMetadata"
# MAGIC %sql
# MAGIC drop table retailer_db.tbl_DeltaCustomerWithUserDefinedMetadata

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY retailer_db.tbl_DeltaCustomerWithUserDefinedMetadata;

# COMMAND ----------

# DBTITLE 1,Create a UDF that Returns an Unique Integer Number
import random

def getRandomInt ():
    # Generate Random Integer from 0 to 10000
    return random.randint(0, 100000)

# COMMAND ----------

# DBTITLE 1,"Load" "Each" of the "Data" into the "Delta Table" "retailer_db.tbl_DeltaCustomerWithUserMetadataAndIdempotentWrite" "Exactly Once"
ddl = """c_customer_sk long comment \"This is the Primary Key\",
         c_customer_id string,
         c_current_cdemo_sk long,
         c_current_hdemo_sk long,
         c_current_addr_sk long,
         c_first_shipto_date_sk long,
         c_first_sales_date_sk long,
         c_salutation string,
         c_first_name string,
         c_last_name string,
         c_preferred_cust_flag string,
         c_birth_day int,
         c_birth_month int,
         c_birth_year int,
         c_birth_country string,
         c_login string,
         c_email_address string,
         c_last_review_date long"""

df_ReadCustomerFileUsingCsv = spark.read\
                                   .option("header", "true")\
                                   .schema(ddl)\
                                   .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

appId = "csvFileReadingJob"

df_ReadCustomerFileUsingCsv.write.format("delta")\
                                 .mode("append")\
                                 .option("txnAppId", appId)\
                                 .option("txnVersion", getRandomInt())\
                                 .option("userMetadata", "example-of-table-creation-with-user-defined-metadata")\
                                 .option("path", "/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/idempotent-and-user-metadata-write-on-delta-table")\
                                 .saveAsTable("retailer_db.tbl_DeltaCustomerWithUserMetadataAndIdempotentWrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retailer_db.tbl_DeltaCustomerWithUserMetadataAndIdempotentWrite;

# COMMAND ----------

# DBTITLE 1,Retrieve the "Full History" of a "Delta Table", Defined in the "Metastore", Using Spark SQL
# MAGIC %sql
# MAGIC DESCRIBE HISTORY retailer_db.tbl_DeltaCustomerWithUserMetadataAndIdempotentWrite;
