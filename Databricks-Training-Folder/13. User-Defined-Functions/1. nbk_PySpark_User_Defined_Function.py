# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To PySpark User Defined Functions
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame and Add a New Column Using a PySpark User Defined Function
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is a "PySpark User Defined Function"?
# MAGIC * A "<b>PySpark User Defined Function</b>" i.e., a "<b>PySpark UDF</b>" is a "<b>Regular Custom Python Function</b>" that is "<b>Converted</b>" to "<b>PySpark UDF</b>" using the "<b>udf ()</b>" Function from the "<b>pyspark.sql.functions</b>" Package, so that the "<b>Regular Custom Python Function</b>" can become a "<b>Re-Usable Function</b>" that can be used on "<b>Each Row</b>" of the "<b>Multiple DataFrames</b>".
# MAGIC * A "<b>PySpark UDF</b>" is created "<b>Only Once</b>", but, can be "<b>Re-Used</b>" on "<b>Each Row</b>" of the "<b>Multiple DataFrames</b>" and in "<b>SQL Expressions</b>".
# MAGIC * The "<b>PySpark UDFs</b>" are "<b>Error-Prone</b>". Hence, the "<b>PySpark UDFs</b>" need to be "<b>Designed Carefully</b>".
# MAGIC * If "<b>Not Carefully Created</b>", using a "<b>PySpark UDF</b>" can lead to "<b>Optimization</b>" and "<b>Performance Issues</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why a "PySpark User Defined Function" is Required?
# MAGIC * There may be "<b>Situations</b>", where the "<b>Spark SQL Built-In Functions</b>" are "<b>Not Sufficient Enough</b>" to "<b>Solve</b>" the "<b>Problems</b>".
# MAGIC * In such "<b>Situations</b>", "<b>Apache Spark</b>" "<b>Allows</b>" to "<b>Register</b>" the "<b>Regular Custom Python Functions</b>" as the "<b>PySpark User Defined Functions</b>" to "<b>Solve</b>" the "<b>Problems</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Create a "PySpark User Defined Function"?
# MAGIC * To "<b>Create</b>" a "<b>PySpark User Defined Function</b>", i.e., "<b>PySpark UDF</b>", the "<b>udf ()</b>" Function from the "<b>pyspark.sql.functions</b>" Package needs to be used to "<b>Convert</b>" a "<b>Regular Custom Python Function</b>" to a "<b>PySpark UDF</b>".
# MAGIC * The "<b>udf ()</b>" Function from the "<b>pyspark.sql.functions</b>" Package takes "<b>Two Parameters</b>" -
# MAGIC <br>1. "<b>Name</b>" of the "<b>Regular Custom Python Function</b>" to be "<b>Converted</b>" as a "<b>PySpark UDF</b>".
# MAGIC <br>2. The "<b>Return Type</b>" of the "<b>Regular Custom Python Function</b>" to be "<b>Converted</b>" as a "<b>PySpark UDF</b>".
# MAGIC * The "<b>Default Return Type</b>" of a "<b>PySpark UDF</b>" is "<b>String</b>".

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
ddl_IncomeBandSchema = "ib_lower_band_sk int, ib_lower_bound int, ib_upper_bound int"

df_ReadIncomeBandUsingCsv = spark.read\
                                 .csv(path = "dbfs:/FileStore/tables/retailer/data/income_band.dat", sep = "|", header = True, schema = ddl_IncomeBandSchema)
display(df_ReadIncomeBandUsingCsv)

# COMMAND ----------

# DBTITLE 1,Create a "Regular Custom Python Function"
# Return "FirstIncomeGroup", if the Value Passed to the Function is Between "0" and "59999".
# Return "SecondIncomeGroup", if the Value Passed to the Function is Between "60000" and "119999".
# Return "ThirdIncomeGroup", if the Value Passed to the Function is Between "120000" and "199999".
def findIncomeGroup(value):
    incomeGroup = ""
    if (value >= 0) & (value < 60000):
        incomeGroup = "FirstIncomeGroup"
    elif (value >= 60000) & (value < 120000):
        incomeGroup = "SecondIncomeGroup"
    elif (value >= 120000) & (value < 200000):
        incomeGroup = "ThirdIncomeGroup"
    else:
        incomeGroup = "NA"

    return incomeGroup

# COMMAND ----------

# DBTITLE 1,Convert a "Regular Custom Python Function" to a "PySpark UDF"
from pyspark.sql.functions import *
from pyspark.sql.types import *

udfIncomeGroup = udf(findIncomeGroup, StringType())

# COMMAND ----------

# DBTITLE 1,Add New Derived Column Using "PySpark UDF" Inside the "withColumn" Method of "DataFrame"
df_IncomeBandWithIncomeGroup = df_ReadIncomeBandUsingCsv\
                                  .withColumn("IncomeGroup", udfIncomeGroup(col("ib_upper_bound")))
display(df_IncomeBandWithIncomeGroup)

# COMMAND ----------

# DBTITLE 1,Add New Derived Column Using "PySpark UDF" Inside the "select ()" Method of "DataFrame"
df_IncomeBandWithIncomeGroup = df_ReadIncomeBandUsingCsv\
                                  .select("*", udfIncomeGroup(col("ib_upper_bound")).alias("IncomeGroup"))
display(df_IncomeBandWithIncomeGroup)

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Use a "PySpark User Defined Function" on a "SQL Expression"

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to "Register" a "PySpark UDF" to "Use" it on "SQL Expressions"?
# MAGIC * To "<b>Apply</b>" a "<b>PySpark User Defined Function</b>", i.e., "<b>PySpark UDF</b>" on "<b>Each Row</b>" of a "<b>View</b>", or, a "<b>Table</b>", the "<b>PySpark UDF</b>" needs to be "<b>Registered</b>" to "<b>Apache Spark</b>" using the "<b>spark.udf.register()</b>" Method.
# MAGIC * The "<b>spark.udf.register()</b>" Method takes "<b>Three Parameters</b>" -
# MAGIC <br>1. "<b>Name</b>" of the "<b>PySpark UDF</b>" to be "<b>Created</b>", or, "<b>Already Created</b>".
# MAGIC <br>2. "<b>Name</b>" of the "<b>Regular Custom Python Function</b>" to be "<b>Converted</b>", or, "<b>Already Converted</b>" as a "<b>PySpark UDF</b>".
# MAGIC <br>3. The "<b>Return Type</b>" of the "<b>Regular Custom Python Function</b>" to be "<b>Converted</b>", or, "<b>Already Converted</b>" as a "<b>PySpark UDF</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Temporary View" from a "DataFrame"
df_ReadIncomeBandUsingCsv.createOrReplaceTempView("vw_incomeband")
display(spark.sql("SELECT * FROM vw_incomeband"))

# COMMAND ----------

# DBTITLE 1,Not Possible to Use a "PySpark UDF" that is "Not Registered" to "Apache Spark" on a "SQL Expression"
# It is Not Possible to Use a "PySpark UDF", that is "Not Yet Registered" to "Apache Spark", on a "SQL Expression". It will "Throw" an "AnalysisException".
display(spark.sql("SELECT *, udfIncomeGroup(ib_upper_bound) FROM vw_incomeband"))

# COMMAND ----------

# DBTITLE 1,"Register" a "Regular Custom Python Function" to a "PySpark UDF" to be Used on "SQL Expression"
spark.udf.register("udfIncomeGroup", findIncomeGroup, StringType())

# COMMAND ----------

# DBTITLE 1,Use a "Registered PySpark UDF" to "Apache Spark" on "SQL Expression"
display(spark.sql("SELECT *, udfIncomeGroup(ib_upper_bound) as `Income Group` FROM vw_incomeband"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Why "PySpark User Defined Functions" are "Slow"?
# MAGIC * "<b>Main Reason</b>" for a "<b>PySpark User Defined Function</b>" to be "<b>Slow</b>" is the back and forth "<b>Serialization</b>" and "<b>Deserialization</b>" between the "<b>JVM</b>", and, the "<b>Python Runtime</b>"  -
# MAGIC   * "<b>Spark Engine</b>" is actually "<b>Implemented</b>" in "<b>Two Languages</b>", i.e., "<b>Java</b>" and "<b>Scala</b>". So, "<b>Only</b>" these "<b>Two Languages</b>" can "<b>Run</b>" on the "<b>JVM</b>", which is on the "<b>Executor Node</b>" of a "<b>Spark Cluster</b>".
# MAGIC   * When a "<b>User Defined Function</b>" is "<b>Created</b>" in "<b>Python</b>", that "<b>User Defined Function</b>" can "<b>Not</b>" be "<b>Directly Executed</b>" in the "<b>JVM</b>".
# MAGIC   * A "<b>User Defined Function</b>" that is "<b>Created</b>" in "<b>Python</b>", needs "<b>Python Runtime</b>" to be "<b>Executed</b>". So, there must be an "<b>Interaction</b>" between the "<b>JVM</b>" and the "<b>Python Runtime</b>" for a "<b>PySpark User Defined Function</b>" to be "<b>Executed</b>".
# MAGIC   * "<b>Each</b>" of the "<b>Executor Node</b>" in a "<b>Spark Cluster</b>" has "<b>Python Runtime</b>" in it to "<b>Execute</b>" the "<b>Python Codes</b>". The "<b>Interaction</b>" between the "<b>JVM</b>" and the "<b>Python Runtime</b>" in any "<b>Executor Node</b>" happens via the "<b>Library</b>", called "<b>py4j</b>".
# MAGIC   * The "<b>py4j Library</b>" actually "<b>Enables</b>" the "<b>Python Codes</b>", that is "<b>Running</b>" in a "<b>Python Interpreter</b>", to "<b>Dynamically Access</b>" the "<b>Java Objects</b>" from the "<b>JVM</b>". Using the "<b>py4j Library</b>", the "<b>Python Methods</b>" are able to "<b>Call Codes</b>" from the "<b>JVM</b>" as if the "<b>Java Objects</b>" reside in the "<b>Python Interpreter</b>", and, the "<b>Java Collections</b>" can be "<b>Accessed</b>" through the "<b>Standard Python Collection Methods</b>".
# MAGIC   * Now, "<b>Each Row</b>" of the "<b>DataFrame</b>" would be "<b>Serialized</b>" from "<b>Scala</b>" to "<b>Python</b>", and, "<b>Sent</b>" to the "<b>Python Runtime</b>" so that the "<b>PySpark User Defined Function</b>" can be "<b>Used</b>" on "<b>Each Row</b>" of the "<b>DataFrame</b>". Once "<b>Successfully Applied</b>", the "<b>Result</b>" of the "<b>PySpark User Defined Function</b>" is "<b>Deserialized</b>" from "<b>Python</b>" to "<b>Scala</b>", and, "<b>Sent Back</b>" to the "<b>JVM</b>".
# MAGIC   * This back and forth "<b>Serialization</b>" and "<b>Deserialization</b>" between the "<b>JVM</b>", and, the "<b>Python Runtime</b>" is a "<b>Costly Operation</b>". This is the "<b>Main Reason</b>" of a "<b>PySpark User Defined Function</b>" to be "<b>Slow</b>".
# MAGIC * * "<b>Another Reason</b>" for a "<b>PySpark User Defined Function</b>" to be "<b>Slow</b>" is the "<b>Catalyst Optimizer</b>" does "<b>Not Process</b>" the "<b>PySpark User Defined Function</b>" -
# MAGIC   * The "<b>Catalyst Optimizer</b>" can "<b>Not Process</b>" the "<b>PySpark User Defined Function</b>" at all, because, the "<b>Catalyst Optimizer</b>" treats the "<b>PySpark User Defined Function</b>" as a "<b>Black Box</b>". Therefore the "<b>Catalyst Optimizer</b>" can "<b>Not Apply</b>" any "<b>Optimization</b>" on the "<b>Data</b>" to be "<b>Processed</b>" by a "<b>PySpark User Defined Function</b>", like - "<b>Predicate Pushdown</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why the "Spark SQL Built-In Functions" are "Faster" than the "PySpark User Defined Functions"?
# MAGIC * When a "<b>Transformation Operation</b>" is "<b>Applied</b>" on a "<b>DataFrame</b>" using the "<b>Spark SQL Built-In Functions</b>", that "<b>Transformation Operation</b>" will be "<b>Directly Executed</b>" on the "<b>JVM</b>" of the "<b>Executor Node</b>" of a "<b>Spark Cluster</b>" itself, where the "<b>Implementations</b>" of the "<b>Spark SQL Built-In Functions</b>" reside.
# MAGIC * So, there is "<b>No Extra</b>" "<b>Serialization</b>", and, "<b>Deserialization</b>" Process happening for the "<b>Transformation Operation</b>" using the "<b>Spark SQL Built-In Functions</b>". This is the reason the "<b>Spark SQL Built-In Functions</b>" are "<b>Faster</b>" than the "<b>PySpark User Defined Functions</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # Why the "Scala User Defined Functions" are "Faster" than the "PySpark User Defined Functions"?
# MAGIC * When a "<b>User Defined Function</b>" is "<b>Created</b>" in "<b>Scala</b>", it does "<b>Not Need</b>" the "<b>Python Runtime</b>" to be "<b>Executed</b>" in the "<b>JVM</b>" of the "<b>Executor Node</b>" of a "<b>Spark Cluster</b>", because, "<b>Scala</b>" can "<b>Run</b>" on the "<b>JVM</b>" "<b>Directly</b>".
# MAGIC * Although a "<b>Compilation</b>" is still needed for a "<b>Scala User Defined Function</b>", but, there is "<b>No Extra</b>" "<b>Serialization</b>", and, "<b>Deserialization</b>" Process happening for the "<b>Scala User Defined Function</b>".
# MAGIC * Hence, the "<b>Scala User Defined Functions</b>" are "<b>Slower</b>" than the "<b>Spark SQL Built-In Functions</b>", but, are "<b>Faster</b>" than the "<b>PySpark User Defined Functions</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Issues of PySpark UDF
# MAGIC * "<b>Apache Spark</b>" can "<b>Not Apply</b>" any "<b>Optimization</b>" on the "<b>Data</b>" to be "<b>Processed</b>" by a "<b>PySpark UDF</b>", because, "<b>PySpark UDFs</b>" are "<b>Black Box</b>" to "<b>Apache Spark</b>".
# MAGIC * Hence, when possible, it is always "<b>Recommended</b>" to use the "<b>Spark SQL Built-In Functions</b>" as these "<b>Functions</b>" provide "<b>Optimization</b>".
# MAGIC * The "<b>PySpark UDFs</b>" should "<b>Only be Used</b>" when the "<b>Required Functionality</b>" does "<b>Not Exists</b>" in the "<b>Spark SQL Built-In Functions</b>".
