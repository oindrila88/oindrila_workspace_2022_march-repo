# Databricks notebook source
# MAGIC %md
# MAGIC # Create, Drop and Change the Values of Columns of a DataFrame in Databricks
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Add Columns to that DataFrame in Different Scenarios. Also, Change and Drop Existing Columns of that DataFrame.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
ddl_IncomeBandSchema = "ib_lower_band_sk long, ib_lower_bound int, ib_upper_bound int"

df_ReadIncomeBandUsingCsv = spark.read\
                                 .option("header", "true")\
                                 .option("sep", "|")\
                                 .schema(ddl_IncomeBandSchema)\
                                 .csv("dbfs:/FileStore/tables/retailer/data/income_band.dat")
display(df_ReadIncomeBandUsingCsv)

# COMMAND ----------

# DBTITLE 1,Add Columns Using "withColumn" Method of "DataFrame"
# Add the following columns -
# 1. "isFirstIncomeGroup" With Value as "true" Having Value Between "0" and "59999".
# 2. "isSecondIncomeGroup" With Value as "true" Having Value Between "60000" and "119999".
# 3. "isThirdIncomeGroup" With Value as "true" Having Value Between "120000" and "199999".
from pyspark.sql.functions import col, column

df_IncomeBandWithIncomeGroup = df_ReadIncomeBandUsingCsv\
                                  .withColumn("isFirstIncomeGroup", col("ib_upper_bound") < 60000)\
                                  .withColumn("isSecondIncomeGroup", (col("ib_upper_bound") >= 60000) & (col("ib_upper_bound") < 120000))\
                                  .withColumn("isThirdIncomeGroup", (column("ib_upper_bound") >= 120000) & (column("ib_upper_bound") <= 200000))

display(df_ReadIncomeBandUsingCsv)

display(df_IncomeBandWithIncomeGroup)

# COMMAND ----------

# DBTITLE 1,Add Columns Using "withColumn" Method of "DataFrame" Along With "case()" and "otherwise()" Functions
# Add the following columns -
# 1. "isFirstIncomeGroup" With Value as "Yes" Having Value Between "0" and "59999". Otherwise "No".
# 2. "isSecondIncomeGroup" With Value as "Yes" Having Value Between "60000" and "119999". Otherwise "No".
# 3. "isThirdIncomeGroup" With Value as "Yes" Having Value Between "120000" and "199999". Otherwise "No".
from pyspark.sql.functions import col, when

df_IncomeBandWithIncomeGroupUsingCaseWhen = df_ReadIncomeBandUsingCsv\
                    .withColumn("isFirstIncomeGroup", when(col("ib_upper_bound") < 60000, "Yes")\
                                                      .otherwise("No"))\
                    .withColumn("isSecondIncomeGroup", when((col("ib_upper_bound") >= 60000) & (col("ib_upper_bound") < 120000), "Yes")\
                                                       .otherwise("No"))\
                    .withColumn("isThirdIncomeGroup", when((col("ib_upper_bound") >= 120000) & (col("ib_upper_bound") <= 200000), "Yes")\
                                                      .otherwise("No"))

display(df_IncomeBandWithIncomeGroupUsingCaseWhen)

# COMMAND ----------

# DBTITLE 1,Add Columns Using "withColumn" Method of "DataFrame" Along With "lit()" Functions
from pyspark.sql.functions import lit

df_DemoColumnsWithLit = df_ReadIncomeBandUsingCsv\
                                                  .withColumn("demoString", lit("demo"))\
                                                  .withColumn("demoInteger", lit(1))
display(df_DemoColumnsWithLit)

# COMMAND ----------

# DBTITLE 1,Change the Value of An Already Existing Column in a DataFrame Using "withColumn" Method of "DataFrame"
from pyspark.sql.functions import lit

display(\
        df_DemoColumnsWithLit\
                            .withColumn("demoString", lit("newDemo"))\
                            .withColumn("demoInteger", lit(2))
        )

display(df_DemoColumnsWithLit)

# COMMAND ----------

# DBTITLE 1,Change the Name of An Already Existing Column in a DataFrame Using "withColumnRenamed" Method of "DataFrame"
# Change the Column Name "isThirdIncomeGroup" to "isHighIncomeClass" of "df_IncomeBandWithIncomeGroup" DataFrame
df_IncomeClass = df_IncomeBandWithIncomeGroup\
                    .withColumnRenamed("isFirstIncomeGroup", "isStandardIncomeClass")\
                    .withColumnRenamed("isSecondIncomeGroup", "isMediumIncomeClass")\
                    .withColumnRenamed("isThirdIncomeGroup", "isHighIncomeClass")

# The Column Name "isThirdIncomeGroup" Remains the Same in the "df_IncomeBandWithIncomeGroup" DataFrame
df_IncomeBandWithIncomeGroup.printSchema()

# The Column Name "isThirdIncomeGroup" is Changed to "isHighIncomeClass" in the "df_IncomeClass" DataFrame
df_IncomeClass.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Single Column from a DataFrame Using "drop()" Method Using String Representation of Column Names
 from pyspark.sql.functions import col

df_IncomeBandAfterColumnDropUsingStringColumnName = df_DemoColumnsWithLit.drop("demoString")

# The Column "demoString" Still Remains in the "df_DemoColumnsWithLit" DataFrame
df_DemoColumnsWithLit.printSchema()

# The Column "demoString" is Not Present in the "df_IncomeBandAfterColumnDropUsingStringColumnName" DataFrame
df_IncomeBandAfterColumnDropUsingStringColumnName.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Single Column from a DataFrame Using "drop()" Method Using Column Object Reference
from pyspark.sql.functions import col

df_IncomeBandAfterColumnDropUsingColObj = df_DemoColumnsWithLit.drop(col("demoString"))

# The Column "demoString" Still Remains in the "df_DemoColumnsWithLit" DataFrame
df_DemoColumnsWithLit.printSchema()

# The Column "demoString" is Not Present in the "df_IncomeBandAfterColumnDropUsingColObj" DataFrame
df_IncomeBandAfterColumnDropUsingColObj.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Multiple Columns from a DataFrame Using "drop()" Method Using String Representation of Column Names
df_IncomeBandAfterMultColumnDropUsingStringColumnName = df_DemoColumnsWithLit.drop("demoString", "demoInteger")

# The Columns "demoString" and "demoInteger" Still Remain in the "df_DemoColumnsWithLit" DataFrame
df_DemoColumnsWithLit.printSchema()

# The Columns "demoString" and "demoInteger" are Not Present in the "df_IncomeBandAfterMultColumnDropUsingStringColumnName" DataFrame
df_IncomeBandAfterMultColumnDropUsingStringColumnName.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Multiple Column from a DataFrame Using "drop()" Method Using Multiple Column Object Reference - Not Allowed
from pyspark.sql.functions import col, column

# "drop ()" Method doesn't allow "Multiple Column Object Reference".
df_IncomeBandAfterMultiColumnDropUsingColObj = df_DemoColumnsWithLit.drop(col("demoString"), column("demoInteger"))

# The Columns "demoString" and "demoInteger" Still Remain in the "df_DemoColumnsWithLit" DataFrame
df_DemoColumnsWithLit.printSchema()

# The Columns "demoString" and "demoInteger" are Not Present in the "df_IncomeBandAfterMultiColumnDropUsingColObj" DataFrame
df_IncomeBandAfterMultiColumnDropUsingColObj.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove Multiple Column from a DataFrame Using "drop()" Method By Passing List of Columns to Drop
listOfColumnsToDrop = ["demoString", "demoInteger"]

df_IncomeBandAfterMultColumnDropUsingColumnNamesInList = df_DemoColumnsWithLit.drop(*listOfColumnsToDrop)

# The Columns "demoString" and "demoInteger" Still Remain in the "df_DemoColumnsWithLit" DataFrame
df_DemoColumnsWithLit.printSchema()

# The Columns "demoString" and "demoInteger" are Not Present in the "df_IncomeBandAfterMultColumnDropUsingColumnNamesInList" DataFrame
df_IncomeBandAfterMultColumnDropUsingColumnNamesInList.printSchema()

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .option("inferSchema", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)
df_ReadCustomerFileUsingCsv.printSchema()

# COMMAND ----------

# DBTITLE 1,Change Datatype of Columns Using "withColumn" Method of "DataFrame" Along With "cast()" Function
from pyspark.sql.types import *
from pyspark.sql.functions import *

df_ChangeColumnDataType = df_ReadCustomerFileUsingCsv\
                                                    .withColumn("c_first_shipto_date_sk", col("c_first_shipto_date_sk").cast(StringType()))\
                                                    .withColumn("c_first_name", col("c_first_name").cast("integer"))
display(df_ChangeColumnDataType)

df_ReadCustomerFileUsingCsv.printSchema()

df_ChangeColumnDataType.printSchema()
