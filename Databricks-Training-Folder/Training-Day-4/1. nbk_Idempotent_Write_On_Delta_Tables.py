# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: "Idempotent Writes" on the "Delta Tables"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction to the "Idempotent Writes" on the "Delta Tables"
# MAGIC * Sometimes a "Job" that "Writes" the "Data" to a "Delta Table" is "Restarted" Due to "Various Reasons" (e.g., the "Job" Encounters a "Failure").
# MAGIC * The "Failed Job" "May" or "May Not" have "Written" the "Data" to the "Delta Table" before "Terminating".
# MAGIC * In the case, where the "Data" is "Written" to the "Delta Table", the "Restarted Job" "Writes" the "Same Data" to the "Delta Table", which "Results" in "Duplicate Data".
# MAGIC * To "Address" this problem, "Delta Tables" Support the following "DataFrameWriter" "Options" to make the "Writes" "Idempotent" -
# MAGIC * A. <b>txnAppId</b>: A "Unique String" that can be "Passed" to "Each" of the "DataFrame" "Write", e.g., this can be the "Name" of the "Job".
# MAGIC * B. <b>txnVersion</b>: A "Monotonically Increasing Number" that acts as "Transaction Version". This "Number" needs to be "Unique" for the "Data" that is being "Written" to the "Delta Table(s)", e.g., this can be the "Epoch Seconds" of the "Instant" when the "Query" is "Attempted" for the "First Time". Any "Subsequent Restarts" of the "Same Job" needs to have the "Same Value" for "txnVersion".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Combination" of "txnAppId" and "txnVersion" Needs to be "Unique" for the "Idempotent Writes" on the "Delta Tables"
# MAGIC * The "Combination" of the "txnAppId" and "txnVersion" Options needs to be "Unique" for "Each New Data" that is being "Ingested" into the "Delta Table" and the "txnVersion" Option needs to be "Higher" than the "Last Data" that was "Ingested" into the "Delta Table".
# MAGIC * <b>Example</b>:
# MAGIC * If the "Last Successfully Written Data" "Contains" the Option Values as "<b>dailyETL</b>:<b>23423</b>" ("<b>txnAppId</b>":"<b>txnVersion</b>"), then, the "Next Write" of the "Data" should have "<b>txnAppId</b>" = "<b>dailyETL</b>" and "<b>txnVersion</b>" as "At Least" "<b>23424</b>" ("One More Than" the "txnVersion" Option of the "Last Successfully Written Data").
# MAGIC * Any "Attempt" to "Write" the "Data" with "<b>txnAppId</b>" = "<b>dailyETL</b>" and "<b>txnVersion</b>" as "<b>23422</b>", .i.e., "Less Than" the "txnVersion" Option of the "Last Successfully Written Data" in the table is "Ignored".
# MAGIC * "Attempt" to "Write" the "Data" with "<b>txnAppId</b>":"<b>txnVersion</b>" as "<b>anotherETL</b>":"<b>23424</b>" to the "Delta Table" is "Successful" as it "Contains" a "Different" "txnAppId" Option "Compared" to the "Same Option Value" in the "Last Ingested Data".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Important Feature" of the "txnAppId" and "txnVersion" Options
# MAGIC * Using the "Combination" of the "txnAppId" and "txnVersion" Options "Assumes" that the "Data" being "Written" to the "Delta Table(s)" in "Multiple Retries" of the "Job" is "Same".
# MAGIC * If a "Write Attempt" in a "Delta Table" "Succeeds" but due to some "Downstream Failure" there is a "Second" "Write Attempt" with "Same" "txnAppId" and "txnVersion" Options, but "Different Data", then that "Second Write Attempt" will be "Ignored". This can "Cause" "Unexpected Results".

# COMMAND ----------

# DBTITLE 1,Create a UDF that Returns a UUID
import uuid

def getUuid ():
    return str(uuid.uuid4())

# COMMAND ----------

# DBTITLE 1,"Load" "Each" of the "Data" into the "Delta Table" "retailer_db.tbl_DeltaCustomerWithIdempotentWrite" "Exactly Once"
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

app_id = "csvFileReadingJob"
txn_version = getUuid()

df_ReadCustomerFileUsingCsv.write.format("delta")\
                                 .mode("append")\
                                 .option("txnAppId", app_id)\
                                 .option("txnVersion", txn_version)\
                                 .option("path", "/mnt/with-aad-app/databricks-training-folder/day-4/target-delta-table/idempotent-write-on-delta-table")\
                                 .saveAsTable("retailer_db.tbl_DeltaCustomerWithIdempotentWrite")
