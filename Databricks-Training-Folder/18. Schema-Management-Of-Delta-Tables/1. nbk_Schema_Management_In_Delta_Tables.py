# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Enforcement and Schema Evolution of Delta Tables in Databricks
# MAGIC * Topic: Introduction to "Schema Management" in "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Schema" is "Managed" in "Delta Tables"
# MAGIC * Every "Delta Table" in "Databricks" Contains a "Schema", which is a "Blueprint" that "Defines" the "Shape" of the Data, such as "Data Types" and "Columns", and "Metadata". With "Delta Lake", the "Table’s Schema" is "Saved" in "JSON Format" Inside the "Transaction Log".
# MAGIC * As "Business Problems" and "Requirements" Evolve Over Time, so too does the "Structure" of the Data. With "Delta Lake", as the Data "Changes", "Incorporating" the "New Dimensions" is "Easy". "Users" have "Access" to "Simple Semantics" to "Control" the "Schema" of the "Delta Tables" in the following two ways -
# MAGIC * <b>A. Schema Enforcement:</b> The "Schema Enforcement" Prevents the "Users" from "Accidentally Polluting" the "Delta Tables" with "Mistakes" or "Garbage Data".
# MAGIC * <b>B. Schema Evolution:</b> The "Schema Evolution" Enables the "Users" to "Automatically Add New Columns" of "Rich Data" when those "New Columns" Belong.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Schema Enforcement"?
# MAGIC * "Schema Enforcement", also Known as "Schema Validation", is a "Safeguard" in "Delta Lake" that "Ensures" the "Data Quality" by "Rejecting" the "Writes" to a "Delta Table" that "Do Not Match" the "Delta Table’s" Schema.
# MAGIC * "Schema Enforcement" Checks to see Whether "Each Column" in Data "Inserted" into the "Delta Table" is on its "List" of "Expected Columns" and "Rejects" any "Writes" with the "Columns" that "Aren’t" on the "List".

# COMMAND ----------

# MAGIC %md
# MAGIC # How Does "Schema Enforcement" Work?
# MAGIC * "Delta Lake" uses "Schema Validation" on "Write", which means that "All" the "New Writes" to a "Table" are "Checked" for "Compatibility" with the "Target Table’s Schema" at "Write Time".
# MAGIC * If the "Schema" is "Not Compatible", "Delta Lake" Cancels the "Transaction" Altogether ("No Data" is "Written"), and "Raises" an "Exception" to "Let" the "Users" know about the "Mismatch".
# MAGIC * To "Determine" Whether a "Write" to a "Delta Table" is "Compatible", "Delta Lake" Uses the following "Rules" on the "DataFrame" to be "Written" -
# MAGIC * <b>A. "Incoming DataFrame" Can "Not Contain" any "Additional Columns" that are "Not Present" in the "Target Table’s Schema":</b> It is "OK" If the "Incoming DataFrame" "Doesn’t Contain" "Every Column" in the "Target Delta Table". In such cases, those "Columns", which are Present in the "Target Delta Table" but "Not" in the "Incoming DataFrame", will Simply be "Assigned" the "Null Values".
# MAGIC * <b>B. "Incoming DataFrame" Can "Not Have" the "Column Data Types" that "Differ" from the "Column Data Types" in the "Target Delta Table":</b>. If a "Target Delta Table’s Column" Contains "StringType" Data, but the Corresponding "Column" in the "Incoming DataFrame" Contains "IntegerType" Data, then "Schema Enforcement" will "Raise" an "Exception" and "Prevent" the "Write Operation" from Taking Place.
# MAGIC * <b>C. "Incoming DataFrame" Can "Not Contain" the "Column Names" that "Differ" from the "Column Names" in the "Target Delta Table" Only "By Case":</b> This means that there Can "Not Be" Columns, such as 'Name' and 'name' that are "Defined" in the "Same Target Delta Table". "Delta Lake" is "Case-Preserving" but "Case-Insensitive" When "Storing" the "Schema". "Parquet" is "Case-Sensitive" When "Storing" and "Returning" the "Column Information". To "Avoid" the "Potential Mistakes", "Data Corruption" or "Data Loss" issues, this "Restriction" was "Added" to the "Delta Lake".

# COMMAND ----------

# DBTITLE 1,"Incoming DataFrame" Has Column that is "Not Present" in the "Target Delta Table"
df_ReadCsvWithExtraColumn = spark.read.option("header", "true")\
                                      .option("inferSchema", "true")\
                                      .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-enforcement-csv-files/Customer_4.csv")

display(df_ReadCsvWithExtraColumn)

df_ReadCsvWithExtraColumn.write.format("delta")\
                               .mode("append")\
                               .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,"Data Type" of a "Column" in "Incoming DataFrame" "Differ" from the "Data Type" in the "Target Delta Table"
df_ReadCsvWithWrongDataType = spark.read.option("header", "true")\
                                        .option("inferSchema", "true")\
                                        .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-enforcement-csv-files/Customer_5.csv")

display(df_ReadCsvWithWrongDataType)

df_ReadCsvWithWrongDataType.write.format("delta")\
                                 .mode("append")\
                                 .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,"Name" of a "Column" in "Incoming DataFrame" "Differ" from the "Name" in the "Target Delta Table"
# "No New Columns" Will be "Created" in the "Target Delta Table". The Data, in the "Columns" of the "Incoming Dataset" Having a Name that "Differ" from the "Columns" of the "Target Delta Table" Only by a "Case", will be "Loaded" into the Respective "Columns" of the "Target Delta Table".

df_ReadCsvWithWrongColumnName = spark.read.option("header", "true")\
                                          .option("inferSchema", "true")\
                                          .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-enforcement-csv-files/Customer_6.csv")

display(df_ReadCsvWithWrongColumnName)

df_ReadCsvWithWrongColumnName.write.format("delta")\
                                   .mode("append")\
                                   .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,Two "Columns" with the "Same Name" but in "Different Casing" Exist in "Incoming DataFrame"
df_ReadCsvWithSameColumnNameWithDifferentCase = spark.read.option("header", "true")\
                                                     .option("inferSchema", "true")\
                                                     .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-enforcement-csv-files/Customer_7.csv")

display(df_ReadCsvWithSameColumnNameWithDifferentCase)

df_ReadCsvWithSameColumnNameWithDifferentCase.write.format("delta")\
                                             .mode("append")\
                                             .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "training.customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.customers;
# MAGIC 
# MAGIC -- The Data from the "Two Columns", i.e., "first_Name" and "last_Name" of the "Source Dataset", i.e., "df_ReadCsvWithWrongColumnName" got "Loaded" into the "First_Name" and "Last_Name" Columns of the "Target Delta Table" Respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Schema Evolution"?
# MAGIC * "Schema Evolution" is a "Feature" that "Allows" the "Users" to Easily "Change" a "Delta Table’s Current Schema" to "Accommodate" the Data that is "Changing" "Over Time".
# MAGIC * Most commonly, "Schema Evolution" is Used When "Performing" an "Append" or "Overwrite" Operation, to "Automatically Adapt" the "Schema" to "Include" "One" or "More" "New Columns".

# COMMAND ----------

# MAGIC %md
# MAGIC # How Does "Schema Evolution" Work?
# MAGIC * "Developers" can Easily Use the "Schema Evolution" to "Add" the "New Columns" that were "Previously Rejected" Due to a "Schema Mismatch".
# MAGIC * "Schema Evolution" is "Activated" by "Adding" the "<b>.option('mergeSchema', 'true')</b>" to the "<b>.write</b>" or "<b>.writeStream</b>" "Spark Command".
# MAGIC * Alternatively, it is possible to "Set" the Option of the "Schema Evolution" for the "Entire Spark Session" by "Adding" "spark.databricks.delta.schema.autoMerge = True" to the "Spark Configuration".
# MAGIC * By "Including" the "mergeSchema" Option in the Query, Any "Columns" that are "Present" in the "Incoming DataFrame" but "Not" in the "Target Delta Table" are "Automatically Added" on to the "End" of the "Schema" as "Part" of a "Write" Transaction.
# MAGIC * "Nested Fields" can also be "Added", and these "Fields" will get "Added" to the "End" of the Respective "Struct Columns" as well.
# MAGIC * The following types of "Schema Changes" are "Eligible" for "Schema Evolution" During the "Table Appends" or "Overwrites":
# MAGIC * A. "Adding" the "New Columns" (this is the "Most Common Scenario").
# MAGIC * B. "Changing" of "Data Types" from "NullType" to "Any Other Type", or "Upcasts" from "ByteType" -> "ShortType" -> "IntegerType".

# COMMAND ----------

# DBTITLE 1,"Incoming DataFrame" Has Column that is "Not Present" in the "Target Delta Table" - "Schema Got Evolved"
df_ReadCsvWithExtraColumn = spark.read.option("header", "true")\
                                      .option("inferSchema", "true")\
                                      .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-evolution-csv-files/Customer_4.csv")

display(df_ReadCsvWithExtraColumn)

df_ReadCsvWithExtraColumn.write.format("delta")\
                               .option("mergeSchema", "true")\
                               .mode("append")\
                               .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "training.customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC # "Limitations" of "Schema Evolution"
# MAGIC * Some "Changes" are "Not Eligible" for "Schema Evolution", which "Require" that the "Schema" and the "Data" are "Overwritten" by "Adding" the "<b>.option("overwriteSchema", "true")</b>".
# MAGIC * Example - in the case where the "Column" "Age" was Originally an "Integer" Data Type and the "New Schema" would be a "String" Data Type, then "All" of the "Parquet Files" (Data Files) would "Need" to be "Re-Written". Those "Changes" Include:
# MAGIC * A. "Dropping" a "Column".
# MAGIC * B. "Changing" an "Existing Column's Data Type" (In-Place).
# MAGIC * C. "Loading" the Data from a "Column" in "Incoming Dataset" to a "Column" in the "Target Delta Table" that Only "Differs" by "Case".

# COMMAND ----------

# DBTITLE 1,"Data Type" of a "Column" in "Incoming DataFrame" "Differ" from the "Data Type" in the "Target Delta Table"
df_ReadCsvWithWrongDataType = spark.read.option("header", "true")\
                                        .option("inferSchema", "true")\
                                        .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-evolution-csv-files/Customer_5.csv")

display(df_ReadCsvWithWrongDataType)

df_ReadCsvWithWrongDataType.write.format("delta")\
                                 .option("mergeSchema", "true")\
                                 .mode("append")\
                                 .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,"Name" of a "Column" in "Incoming DataFrame" "Differ" from the "Name" in the "Target Delta Table"
# "No New Columns" Will be "Created" in the "Target Delta Table". The Data, in the "Columns" of the "Incoming Dataset" Having a Name that "Differ" from the "Columns" of the "Target Delta Table" Only by a "Case", will be "Loaded" into the Respective "Columns" of the "Target Delta Table".
df_ReadCsvWithWrongColumnName = spark.read.option("header", "true")\
                                          .option("inferSchema", "true")\
                                          .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-evolution-csv-files/Customer_6.csv")

# df_ReadCsvWithWrongColumnName.write.format("delta")\
#                                    .option("mergeSchema", "true")\
#                                    .mode("append")\
#                                    .save("/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/training_customers")

display(df_ReadCsvWithWrongColumnName)

df_ReadCsvWithWrongColumnName.write.format("delta")\
                                   .option("mergeSchema", "true")\
                                   .mode("append")\
                                   .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,Two "Columns" with the "Same Name" but in "Different Casing" Exist in "Incoming DataFrame"
df_ReadCsvWithSameColumnNameWithDifferentCase = spark.read.option("header", "true")\
                                                     .option("inferSchema", "true")\
                                                     .csv("/mnt/with-aad-app/databricks-training-folder/day-4/schema-evolution-csv-files/Customer_7.csv")

display(df_ReadCsvWithSameColumnNameWithDifferentCase)

df_ReadCsvWithSameColumnNameWithDifferentCase.write.format("delta")\
                                             .option("mergeSchema", "true")\
                                             .mode("append")\
                                             .saveAsTable("training.customers")

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "training.customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.customers;
# MAGIC 
# MAGIC -- The "Source Dataset", i.e., "df_ReadCsvWithWrongColumnName" has "Two Columns", i.e., "first_Name" and "last_Name", which "Differs", from the "Two Columns" of the "Target Delta Table", i.e., "First_Name" and "Last_Name", "Only by Case". Hence, "No New Columns", by the Names "first_Name" and "last_Name" got "Created" in the "Target Delta Table", i.e., "Schema Did Not Evolve".
# MAGIC 
# MAGIC -- The Data from the "Two Columns", i.e., "first_Name" and "last_Name" of the "Source Dataset", i.e., "df_ReadCsvWithWrongColumnName" got "Loaded" into the "First_Name" and "Last_Name" Columns of the "Target Delta Table" Respectively.
