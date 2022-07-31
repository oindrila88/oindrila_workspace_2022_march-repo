# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 4
# MAGIC * Topic: "Upsert" Data into a "Delta Table" Using "DeltaTableBuilder API".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,"whenMatched" Clause of the "merge" Programmatic Operation
# MAGIC %md
# MAGIC ## "whenMatched" Clauses are "Executed" When a "Source Row" Matches a "Target Table Row" based on the "Match Condition".
# MAGIC #### The "whenMatched" Clause has the following semantics -
# MAGIC * The "whenMatched" Clause can have "At Most" "One Update" and "One Delete" Action.
# MAGIC * The "Update" Action in "Merge" Only "Updates" the "Specified Columns" (Similar to the "Update Operation") of the "Matched Target Row".
# MAGIC * The "Delete" Action "Deletes" the "Matched Row".
# MAGIC * Each "whenMatched" Clause can have an "Optional Clause Condition". If this "Optional Clause Condition" Exists, the "Update" or "Delete" Action is "Executed" for any "Matching" "Source and Target Row Pair" Only When the "Optional Clause Condition" is "True".
# MAGIC * If there are Multiple "whenMatched" Clauses, then those are "Evaluated" in the "Order" those are "Specified". All "whenMatched" Clauses, "Except" the "Last One", must have "Conditions".
# MAGIC * If "None" of the "whenMatched" Conditions "Evaluate" to "True" for a "Source and Target Row Pair" that "Matches" the "Merge" Condition, then the "Target Row" is "Left Unchanged".
# MAGIC * To "Update" "All" the "Columns" of the "Target Delta Table" with the Corresponding "Columns" of the "Source Dataset", the "whenMatched(...).updateAll()" should be Used. This Action "Assumes" that the "Source Table" has the "Same Columns" as those in the "Target Table", Otherwise the Query "Throws" an "Analysis Error". This Behavior "Changes" When "Automatic Schema Migration" is "Enabled".

# COMMAND ----------

# DBTITLE 1,"whenNotMatched" Clause of the "merge" Programmatic Operation
# MAGIC %md
# MAGIC ## "whenNotMatched" Clauses are "Executed" When a "Source Row" "Does Not Match" any "Target Table Row" based on the "Match Condition".
# MAGIC #### The "whenNotMatched" Clause has the following semantics -
# MAGIC * The "whenNotMatched" Clause can have Only the "Insert" Action. The "New Row" is "Generated" Based on the "Specified Column" and the Corresponding "Expressions". There is "No Need" to "Specify" "All" the "Columns" in the "Target Table". For the "Unspecified Target Columns", NULL is "Inserted".
# MAGIC * Each "whenNotMatched" Clause can have an "Optional Clause Condition". If the "Optional Clause Condition" is "Present", a "Source Row" is "Inserted" Only If that "Condition" is "True" for that "Row". Otherwise, the "Source Column" is "Ignored".
# MAGIC * If there are Multiple "whenNotMatched" Clauses, then those are "Evaluated" in the "Order" those are "Specified". All "whenNotMatched" Clauses, "Except" the "Last One", must have "Conditions".
# MAGIC * To "Insert" "All" the "Columns" of the "Target Delta Table" with the Corresponding "Columns" of the "Source Dataset", the "whenNotMatched(...).insertAll()" should be Used. This Action "Assumes" that the "Source Table" has the "Same Columns" as those in the "Target Table", Otherwise the Query "Throws" an "Analysis Error". This Behavior "Changes" When "Automatic Schema Migration" is "Enabled".

# COMMAND ----------

# MAGIC %md
# MAGIC # Important Points about "merge" Programmatic Operation -
# MAGIC * A "merge" Operation can "Fail" if "Multiple Rows" of the "Source Dataset" "Match" and the "merge" "Attempts" to "Update" the "Same Rows" of the "Target Delta Table". According to the "SQL Semantics" of "merge", such an "Update" Operation is "Ambiguous" as it is "Unclear" which "Source Row" should be Used to "Update" the "Matched Target Row". It is possible to "Pre-Process" the "Source Table" to "Eliminate" the Possibility of "Multiple Matches" Using the "Change Data Capture", which shows "How to Pre-Process" the "Change Dataset" (that is, the "Source Dataset") to "Retain Only" the "Latest Change" for "Each Key" Before "Applying" that "Change" into the "Target Delta Table".
# MAGIC * A "merge" Operation can "Produce" "Incorrect Results" if the "Source Dataset" is "Non-Deterministic". This is because "merge" may "Perform" "Two Scans" of the "Source Dataset" and If the "Data" "Produced" by the "Two Scans" are "Different", the "Final Changes" made to the "Table" can be "Incorrect". "Non-Determinism" in the "Source" can Arise in Many Ways. Some of these are as follows -
# MAGIC * A. "Reading" from "Non-Delta Tables" - Example, "Reading" from a "CSV Table", where the "Underlying Files" can "Change" between the "Multiple Scans".
# MAGIC * B. Using "Non-Deterministic Operations" - Example, "Dataset.filter()" Operations that Uses "Current Timestamp" to "Filter" the Data can Produce "Different Results" Between the "Multiple Scans".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Schema Validation" in "merge" Programmatic Operation -
# MAGIC * "merge" Automatically "Validates" that the "Schema" of the Data "Generated" by "Insert" and "Update" Expressions are "Compatible" with the "Schema" of the "Table". It Uses the Following "Rules" to "Determine" Whether the "merge" Operation is "Compatible" -
# MAGIC * A. For "Update" and "Insert" Actions, the Specified "Target Columns" must "Exist" in the "Target Delta Table".
# MAGIC * B. For "updateAll" and "insertAll" Actions, the "Source Dataset" must have "All" the "Columns" of the "Target Delta Table". The "Source Dataset" can have "Extra Columns" and those are "Ignored".
# MAGIC * For "All Actions", If the "Data Type" "Generated" by the "Expressions" Producing the "Target Columns" are "Different" from the Corresponding "Columns" in the "Target Delta Table", "merge" Tries to "Cast" those to the "Types" in the "Table".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Automatic Schema Evolution" in "merge" Programmatic Operation -
# MAGIC * By default, "updateAll" and "insertAll" Assign "All" the "Columns" in the "Target Delta Table" with "Columns" of the "Same Name" from the "Source Dataset". Any "Columns" in the "Source Dataset" that "Don’t Match" the "Columns" in the "Target Table" are "Ignored". However, in some Use Cases, it is "Desirable" to Automatically "Add" the "Source Columns" to the "Target Delta Table".
# MAGIC * To "Automatically" "Update" the "Table Schema" during a "merge" Operation with "updateAll" and "insertAll" (At Least "One" of those), it is possible to "Set" the "Spark Session Configuration" "spark.databricks.delta.schema.autoMerge.enabled" to "true" Before "Running" the "merge" Operation.
# MAGIC * "Schema Evolution" Occurs "Only When" there is "Either" an "updateAll" ("UPDATE SET *") or an "insertAll" ("INSERT *") Action, or both.
# MAGIC * "update" and "insert" Actions "Cannot Explicitly Refer" to "Target Columns" that "Do Not" Already "Exist" in the "Target Table".

# COMMAND ----------

# DBTITLE 1,"Create" the "DataFrame" for "Third Load" by "Reading" the "Customer_3.csv" File
df_ReadCsvFileForThirdLoad = spark.read.option("header", "true").csv("/mnt/with-aad-app/databricks-training-folder/day-3/upsert-csv-files/Customer_3.csv")
display(df_ReadCsvFileForThirdLoad)

# COMMAND ----------

# DBTITLE 1,"Upsert" the Data into the Delta Table Using "DeltaTableBuilder API"
from delta.tables import *

deltaTableCustomer = DeltaTable.forName(spark, 'training.customers')

deltaTableCustomer.alias('target') \
  .merge(
    df_ReadCsvFileForThirdLoad.alias('source'),
    'target.Customer_Id = source.Customer_Id'
  ) \
  .whenMatchedUpdate(set =
    {
      "target.First_Name": "source.First_Name",
      "target.Last_Name": "source.Last_Name",
      "target.City": "source.City",
      "target.Country": "source.Country"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "target.Customer_Id": "source.Customer_Id",
      "target.First_Name": "source.First_Name",
      "target.Last_Name": "source.Last_Name",
      "target.City": "source.City",
      "target.Country": "source.Country"
    }
  ) \
  .execute()

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "training.customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.customers;
