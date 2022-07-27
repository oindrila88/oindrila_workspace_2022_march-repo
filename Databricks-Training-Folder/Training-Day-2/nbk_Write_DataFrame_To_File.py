# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 2
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Perform Some Transformation on that DataFrame and Create New DataFrame. Write the New DataFrame into a File and Store into DBFS.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a DAT File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerDatFileUsingCsv = spark.read\
                                  .option("sep", "|")\
                                  .option("header", "true")\
                                  .csv("dbfs:/FileStore/tables/retailer/data/customer.dat")

display(df_ReadCustomerDatFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Overwrite Mode"
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("Overwrite")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Append" Mode"
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("Append")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Read "Part0.csv"
df_ReadPart0Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00000-tid-6361461086733544647-4c4763e4-0895-48af-978b-898d6b7517d6-2-1-c000.csv")
df_ReadPart0Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part1.csv"
df_ReadPart1Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00001-tid-6361461086733544647-4c4763e4-0895-48af-978b-898d6b7517d6-3-1-c000.csv")
df_ReadPart1Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part2.csv"
df_ReadPart2Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00002-tid-6361461086733544647-4c4763e4-0895-48af-978b-898d6b7517d6-4-1-c000.csv")
df_ReadPart2Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part3.csv"
df_ReadPart3Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00003-tid-6361461086733544647-4c4763e4-0895-48af-978b-898d6b7517d6-5-1-c000.csv")
df_ReadPart3Csv.count()

# COMMAND ----------

# DBTITLE 1,How Many Partitions Apache Spark DataFrame Contains
df_ReadCustomerDatFileUsingCsv.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Compress the Files
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("Overwrite")\
                              .options(\
                                        header = "true",\
                                        path = "dbfs:/tmp/compressed_output_csv",\
                                        compression = "snappy"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Partition the Output By "Birth Day", "Birth Month" and "Birth Year"
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("Overwrite")\
                              .partitionBy("c_birth_year", "c_birth_month", "c_birth_day")\
                              .options(\
                                        header = "true",\
                                        path = "dbfs:/tmp/partitioned_output_csv",\
                                      )\
                              .save()

# COMMAND ----------

# Delete "Directory" Along with Child "Directories" and "Files".
dbutils.fs.rm("/tmp/output_csv", True)

# Delete "Directory" Along with Child "Directories" and "Files".
dbutils.fs.rm("dbfs:/tmp/compressed_output_csv", True)

# Delete "Directory" Along with Child "Directories" and "Files".
dbutils.fs.rm("dbfs:/tmp/partitioned_output_csv", True)

# COMMAND ----------

dbutils.fs.head("/tmp/compressed_output_csv/part-00000-tid-6922350440045210222-baeafa9c-6a5a-4270-980e-813c73571915-30-1-c000.csv.snappy")
