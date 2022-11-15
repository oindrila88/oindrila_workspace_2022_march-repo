# Databricks notebook source
# MAGIC %md
# MAGIC # Create DataFrames From AVRO File
# MAGIC * Topic: Introduction to "AVRO" File Format.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "AVRO" File Format?
# MAGIC #### "Apache Avro" is a Commonly Used "Data Serialization System" in the "Streaming World" that "Provides" -
# MAGIC * Rich "Data Structures".
# MAGIC * A "Compact", "Fast", "Binary" Data Format.
# MAGIC * A "Container File", to "Store" the "Persistent Data".
# MAGIC * "Remote Procedure Call" (RPC).
# MAGIC * "Simple Integration" with "Dynamic Languages".
# MAGIC * "Code Generation" is "Not Required" to "Read" or "Write" the "Data Files" nor to "Use" or "Implement" the "RPC Protocols". "Code Generation" is an "Optional Optimization", only worth implementing for "Statically Typed Languages".

# COMMAND ----------

# MAGIC %md
# MAGIC # Benefits of Using "AVRO" File Format
# MAGIC #### "Apache Avro" as a "Data Source" Supports thefollowing -
# MAGIC * <b>Schema conversion</b>: "Automatic Conversion" between "Apache Spark SQL" and "Avro" Records.
# MAGIC * <b>Partitioning</b>: Easily "Reading" and "Writing" the "Partitioned Data" Without any "Extra Configuration".
# MAGIC * <b>Compression</b>: "Compression" to Use When "Writing" the "Avro" File "Out" to the "Disk". The "Supported Types" are - "uncompressed", "snappy", and "deflate". It is also possible to "Specify" the "Deflate Level".

# COMMAND ----------

# MAGIC %md
# MAGIC # Where the "AVRO" File Format is Used?
# MAGIC * A Typical Solution is to "Put" the Data in "Avro" Format in "Apache Kafka", "Metadata" in "Confluent Schema Registry", and then "Run" the "Queries" with a "Streaming Framework" that "Connects" to both "Kafka" and "Schema Registry".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Configuring" the "AVRO" Data Source
# MAGIC #### It is possible to "Change" the "Behavior" of an "Avro Data Source" using "Various Configuration Parameters" -
# MAGIC * To "Configure" the "Compression" when "Writing" to the "Avro File", the following "Spark Properties" can be "Set" either in the "Spark Cluster Configuration" or at "Runtime" using "spark.conf.set()" -
# MAGIC * <b>A. Compression Codec</b>: The "Spark Property" "<b>spark.sql.avro.compression.codec</b>" is Used to "Specify" the "Compression Codec" to Use when "Writing" to the "Avro File". "Supported Codecs" are "snappy" and "deflate". The "Default Codec" is "snappy".
# MAGIC * Example - '<b>spark.conf.set("spark.sql.avro.compression.codec", "deflate")</b>'
# MAGIC * <b>B. Compression Level</b>: If the Selected "Compression Codec" is "deflate", the "Compression Level" can be "Set" with the "Spark Property" "<b>spark.sql.avro.deflate.level</b>". The "Default Level" is "-1".
# MAGIC * Example - '<b>spark.conf.set("spark.sql.avro.deflate.level", "4")</b>'
# MAGIC * <b>Merging Schema from Multiple Avro Files</b>: It is possible to "Change" the "Default Schema Inference" Behavior in "Avro" by "Providing" the "<b>mergeSchema</b>" Option when "Reading Files". "Setting" the "<b>mergeSchema</b>" Option to "true" will "Infer" a "Schema" from a "Set of Avro Files" in the "Target Directory" and "Merge" the "Records" rather than "Inferring" a "Schema" from a "Single File".
# MAGIC * Example - '<b>spark.conf.set(spark.databricks.delta.schema.autoMerge, "true")</b>'

# COMMAND ----------

# DBTITLE 1,Read an AVRO File Using "format" method of "DataFrameReader" and Create a DataFrame
df_ReadAvroFile = spark.read.format("avro").load("/mnt/with-aad-app/databricks-training-folder/day-4/autoloader-avro-files/Avro_Document_1.avro")
display(df_ReadAvroFile)
