# Databricks notebook source
# MAGIC %md
# MAGIC # Write the Contents of DataFrames in Databricks
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Perform Some Transformation on that DataFrame and Create New DataFrame. Write the New DataFrame into a File and Store into DBFS Using the Generic "save ()" Method.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Write the Contents of a DataFrame to Any File Format Using Spark?
# MAGIC * In "<b>Spark</b>", it is possible to "<b>Save</b>" the "<b>Contents of a DataFrame</b>" to a "<b>File</b>" of the "<b>Desired Format</b>", to a "<b>Specified Location</b>" of "<b>DBFS</b>", or, "<b>Azure Blob Storage Container</b>", or, "<b>Azure Data Lake Storage Container</b>", by using a "<b>Generic Method</b>", i.e., "<b>save ()</b>".
# MAGIC * The "<b>Format</b>" of the "<b>Files</b>" to be "<b>Written</b>" by the "<b>save ()</b>" Method can be of the "<b>Following Types</b>" -
# MAGIC <br>
# MAGIC   1. <b>CSV</b><br>
# MAGIC   2. <b>Parquet</b><br>
# MAGIC   3. <b>AVRO</b><br>
# MAGIC   4. <b>JSON</b><br>
# MAGIC   5. <b>Text</b><br>
# MAGIC   6. <b>ORC</b><br>
# MAGIC "<b>DataFrameWriter</b>" Class has a method, called "<b>format</b>" to "<b>Specify</b>" the "<b>Format</b>" of the "<b>Files</b>" to be "<b>Written</b>" by the "<b>save ()</b>" Method.
# MAGIC * The "<b>save ()</b>" Method "<b>Does Not Preserve</b>" the "<b>Metadata</b>" and the "<b>Schema</b>" of the "<b>DataFrame</b>".
# MAGIC * The "<b>save ()</b>" Method "<b>Treats</b>" a "<b>DataFrame</b>" as a "<b>Collection of Data</b>", and, "<b>Writes</b>" the "<b>Contents of that DataFrame</b>" to a "<b>File</b>" "<b>Without Considering</b>" any "<b>Table-Related Information</b>".

# COMMAND ----------

# DBTITLE 1,Read a DAT File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerDatFileUsingCsv = spark.read\
                                  .option("sep", "|")\
                                  .option("header", "true")\
                                  .csv("dbfs:/FileStore/tables/retailer/data/customer.dat")

display(df_ReadCustomerDatFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Find "Total Number of Rows" in the Created DataFrame
df_ReadCustomerDatFileUsingCsv.count()

# COMMAND ----------

# DBTITLE 1,Delete All the Files Present in the Path "dbfs:/tmp/output_csv"
dbutils.fs.rm("dbfs:/tmp/output_csv", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What are the Different Saving Modes of the "save ()" Method?
# MAGIC * "<b>DataFrameWriter</b>" Class has a method, called "<b>mode</b>" to "<b>Specify</b>" the "<b>Behaviour</b>" of the "<b>save ()</b>" Method when the "<b>File</b>" to "<b>Write To Already Exists</b>".
# MAGIC * The "<b>Argument</b>" to the "<b>mode</b>" Method "<b>Either Takes</b>" a "<b>String</b>", or, a "<b>Constant</b>" from the "<b>SaveMode<b>" Class.
# MAGIC * There can be "<b>Four Types</b>" of "<b>Saving Mode</b>" when the "<b>File</b>" to "<b>Write To Already Exists</b>", which is to be "<b>Passed</b>" to the "<b>mode</b>" Method. Following are the "<b>Saving Mode</b>" -
# MAGIC <br><br>
# MAGIC 1. <b>Overwrite</b>: The "<b>Overwrite</b>" is the "<b>Saving Mode</b>" to be used, when it is required to "<b>Overwrite</b>" the "<b>Already Existing File</b>".
# MAGIC <br>For "<b>Overwrite</b>" as the "<b>Saving Mode</b>", the "<b>String</b>" to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>overwrite</b>", or, the "<b>Constant</b>" from the "<b>SaveMode</b>" Class to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>SaveMode.Overwrite</b>".
# MAGIC <br><br>
# MAGIC 2. <b>Append</b>: The "<b>Append</b>" is the "<b>Saving Mode</b>" to be used, when it is required to "<b>Append</b>" the "<b>Contents of the DataFrame</b>" to the "<b>Already Existing File</b>".
# MAGIC <br>For "<b>Append</b>" as the "<b>Saving Mode</b>", the "<b>String</b>" to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>append</b>", or, the "<b>Constant</b>" from the "<b>SaveMode</b>" Class to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>SaveMode.Append</b>".
# MAGIC <br><br>
# MAGIC 3. <b>Ignore</b>: The "<b>Ignore</b>" is the "<b>Saving Mode</b>" to be used, when it is required to "<b>Not Write</b>" the "<b>Contents of the DataFrame</b>" to the "<b>Already Existing File</b>". The "<b>Content</b>" of the "<b>Already Existing File</b>" is "<b>Not Changed in Any Way</b>" in this "<b>Saving Mode</b>".
# MAGIC <br>For "<b>Ignore</b>" as the "<b>Saving Mode</b>", the "<b>String</b>" to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>ignore</b>", or, the "<b>Constant</b>" from the "<b>SaveMode</b>" Class to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>SaveMode.Ignore</b>".
# MAGIC <br><br>
# MAGIC 4. <b>ErrorIfExists</b>: The "<b>ErrorIfExists</b>" is the "<b>Saving Mode</b>" to be used, when it is required to "<b>Throw Runtime Exception Message</b>" at the time of "<b>Writing</b>" the "<b>Contents of the DataFrame</b>" to an "<b>Already Existing File</b>".
# MAGIC <br>For "<b>ErrorIfExists</b>" as the "<b>Saving Mode</b>", the "<b>String</b>" to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>errorifexists</b>"/"<b>error</b>", or, the "<b>Constant</b>" from the "<b>SaveMode</b>" Class to "<b>Pass</b>" to the "<b>mode</b>" Method is "<b>SaveMode.ErrorIfExists</b>".
# MAGIC <br> This is the "<b>Default Behaviour</b>" when the "<b>File</b>" to "<b>Write To Already Exists</b>".

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Overwrite Mode" Using "overwrite" String
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("overwrite")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Overwrite Mode" Using "SaveMode.Overwrite" Constant
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode(SaveMode.Overwrite)\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Append" Mode" Using "append" String
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("append")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Write Contents of a DataFrame As "CSV" File in "Append" Mode" Using "SaveMode.Append" Constant
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode(SaveMode.Append)\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Ignore Writing the Contents of a DataFrame As "CSV" File in "Ignore" Mode" Using "ignore" String
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("ignore")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Ignore Writing the Contents of a DataFrame As "CSV" File in "Ignore" Mode" Using "SaveMode.Ignore" Constant
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode(SaveMode.Ignore)\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Throw Error When Writing the Contents of a DataFrame As "CSV" File in "ErrorIfExists" Mode" Using "errorifexists"/"error" String
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode("error")\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Throw Error When Writing the Contents of a DataFrame As "CSV" File in "ErrorIfExists" Mode" Using "SaveMode.ErrorIfExists" Constant
df_ReadCustomerDatFileUsingCsv.write\
                              .format("csv")\
                              .mode(SaveMode.ErrorIfExists)\
                              .options(\
                                       path = "dbfs:/tmp/output_csv",\
                                       header = "true"\
                                      )\
                              .save()

# COMMAND ----------

# DBTITLE 1,Read "Part0.csv"
df_ReadPart0Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00000-tid-5714446476323178933-edd3d2c7-93c6-43d2-a18c-5e0b3b2c3857-31-1-c000.csv")
display(df_ReadPart0Csv)
df_ReadPart0Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part1.csv"
df_ReadPart1Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00001-tid-5714446476323178933-edd3d2c7-93c6-43d2-a18c-5e0b3b2c3857-32-1-c000.csv")
display(df_ReadPart1Csv)
df_ReadPart1Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part2.csv"
df_ReadPart2Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00002-tid-1877786374617020492-dbafb8c5-05ab-4a11-be9f-81930f2f1c7a-4-1-c000.csv")
display(df_ReadPart2Csv)
df_ReadPart2Csv.count()

# COMMAND ----------

# DBTITLE 1,Read "Part3.csv"
df_ReadPart3Csv = spark.read.option("header", "true").csv("/tmp/output_csv/part-00003-tid-1877786374617020492-dbafb8c5-05ab-4a11-be9f-81930f2f1c7a-5-1-c000.csv")
display(df_ReadPart3Csv)
df_ReadPart3Csv.count()

# COMMAND ----------

# DBTITLE 1,Read the "SUCCESS" File
dbutils.fs.head("/tmp/output_csv/_SUCCESS")

# COMMAND ----------

# DBTITLE 1,Read the "Committed" File
dbutils.fs.head("/tmp/output_csv/_committed_5714446476323178933")

# COMMAND ----------

# DBTITLE 1,Read the "Started" File
dbutils.fs.head("/tmp/output_csv/_started_5714446476323178933")

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
