# Databricks notebook source
# MAGIC %md
# MAGIC # Perform UPSERT, i.e., "MERGE" Operation on Delta Tables Using SPARK SQL in Databricks
# MAGIC * Topic: "Upsert" Data into a "Delta Table" Using "SQL Command".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert Data into a Delta Table
# MAGIC * It is possible to "Upsert" the Data from a "Source Table", "View", or "DataFrame" into a "Target Delta Table".
# MAGIC * "Delta Lake" Supports "inserts", "updates" and "deletes" in "MERGE", and it Supports "Extended Syntax" Beyond the "SQL Standards" to Facilitate the "Advanced Use Cases".

# COMMAND ----------

# DBTITLE 1,Create the Database "training"
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS training;

# COMMAND ----------

# DBTITLE 1,"Create" a Delta Table "customers" Under the Database "training"
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS training.customers;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS training.customers
# MAGIC (
# MAGIC     Customer_Id STRING,
# MAGIC     First_Name STRING,
# MAGIC     Last_Name STRING,
# MAGIC     City STRING,
# MAGIC     Country STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/with-aad-app/databricks-training-folder/day-3/target-delta-table/training_customers';

# COMMAND ----------

# DBTITLE 1,"Create" the "DataFrame" for "First Load" by "Reading" the "Customer_1.csv" File
df_ReadCsvFileForFirstLoad = spark.read.option("header", "true").csv("/mnt/with-aad-app/databricks-training-folder/day-3/upsert-csv-files/Customer_1.csv")
display(df_ReadCsvFileForFirstLoad)

# COMMAND ----------

# DBTITLE 1,"Create" a "View" from the DataFrame "df_ReadCsvFileForFirstLoad" for "First Load"
vw_ReadCsvFileForFirstLoad = df_ReadCsvFileForFirstLoad.createOrReplaceTempView("vw_Customer")
display(spark.sql("SELECT * FROM vw_Customer"))

# COMMAND ----------

# DBTITLE 1,"Create" the "DataFrame" for "Second Load" by "Reading" the "Customer_2.csv" File
df_ReadCsvFileForSecondLoad = spark.read.option("header", "true").csv("/mnt/with-aad-app/databricks-training-folder/day-3/upsert-csv-files/Customer_2.csv")
display(df_ReadCsvFileForSecondLoad)

# COMMAND ----------

# DBTITLE 1,"Create" a "View" from the DataFrame "df_ReadCsvFileForFirstLoad" for "Second Load"
vw_ReadCsvFileForSecondLoad = df_ReadCsvFileForSecondLoad.createOrReplaceTempView("vw_Customer")
display(spark.sql("SELECT * FROM vw_Customer"))

# COMMAND ----------

# DBTITLE 1,"Upsert" the Data into the Delta Table Using "SQL Command"
# MAGIC %sql
# MAGIC MERGE INTO training.customers TARGET
# MAGIC USING vw_Customer SOURCE
# MAGIC ON TARGET.Customer_Id = SOURCE.Customer_Id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     TARGET.First_Name = SOURCE.First_Name,
# MAGIC     TARGET.Last_Name = SOURCE.Last_Name,
# MAGIC     TARGET.City = SOURCE.City,
# MAGIC     TARGET.Country = SOURCE.Country
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT
# MAGIC   (
# MAGIC     Customer_Id,
# MAGIC     First_Name,
# MAGIC     Last_Name,
# MAGIC     City,
# MAGIC     Country
# MAGIC   )
# MAGIC   VALUES
# MAGIC   (
# MAGIC     SOURCE.Customer_Id,
# MAGIC     SOURCE.First_Name,
# MAGIC     SOURCE.Last_Name,
# MAGIC     SOURCE.City,
# MAGIC     SOURCE.Country
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Display the Data of the Delta Table "training.customers"
# MAGIC %sql
# MAGIC SELECT * FROM training.customers;

# COMMAND ----------

# DBTITLE 1,Drop the Table "training.customers"
# MAGIC %sql
# MAGIC DROP TABLE training.customers;

# COMMAND ----------

# DBTITLE 1,Drop the Database "training"
# MAGIC %sql
# MAGIC DROP DATABASE training;
