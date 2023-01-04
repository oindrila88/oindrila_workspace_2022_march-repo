# Databricks notebook source
# MAGIC %md
# MAGIC # Generate the "Execution Plan" Using "PySprak" in Databricks
# MAGIC * Topic: Introduction to "Execution Plan"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate "Execution Plan"
# MAGIC * Both the "<b>Logical</b> and <b>Physical Plans</b>" can be "<b>Generated</b>" using "<b>PySpark</b>" by using the "<b>".explain()</b>" Function.
# MAGIC * By default, when "<b>No Argument</b>" is "<b>Supplied</b>", the "<b>".explain()</b>" Function Returns the "<b>Physical Plan</b>".

# COMMAND ----------

# DBTITLE 1,Create a DataFrame by Reading the File "customer.csv"
df_Customer = spark.read\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

display(df_Customer)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame by Reading the File "item.dat"
df_Item = spark.read\
               .options(\
                         header = "true",\
                         delimiter = "|",\
                         inferSchema = "true"\
                       )\
               .csv("dbfs:/FileStore/tables/retailer/data/item.dat")

display(df_Item)

# COMMAND ----------

# DBTITLE 1,Create a DataFrame by Reading the File "store_sales.dat"
df_SalesStore = spark.read\
                     .options(\
                               header = "true",\
                               sep = "|",\
                               inferSchema = "true"\
                             )\
                     .csv("dbfs:/FileStore/tables/retailer/data/store_sales.dat")

display(df_SalesStore)

# COMMAND ----------

# DBTITLE 1,Execute a Query by Creating a DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Display
# 1. The "Customer ID", "First Name", "Last Name" of the "Customers" from the File "customer.csv".
# 2. The "Item ID", "Product Name" of the "Items" bought by "Each" of the Corresponding "Customers" from the File "item.dat".
# 3. The "Net Paid", "Net Paid Incl Tax" for "Each" of the Corresponding "Customers" Where the Customer's "Birth Country" Starts With "J" from the File "store_sales.dat".

windowSpec = Window.partitionBy("ss_customer_sk")

# Create the Final Output DataFrame
df_SalesStoreOutput = df_SalesStore\
                                   .join(df_Customer, df_Customer.c_customer_sk == df_SalesStore.ss_customer_sk, "inner")\
                                   .join(df_Item, df_Item.i_item_sk == df_SalesStore.ss_item_sk, "inner")\
                                   .filter(df_Customer.c_birth_country.startswith("J"))\
                                   .where(df_Customer.c_first_name.isNotNull())\
                                   .where(df_Customer.c_last_name.isNotNull())\
                                   .withColumn("Net Paid by Customer", sum(df_SalesStore.ss_net_paid.cast(IntegerType())).over(windowSpec))\
                                   .withColumn("Net Paid Incl Tax by Customer", sum(df_SalesStore.ss_net_paid_inc_tax.cast(IntegerType())).over(windowSpec))\
                                   .select(df_Customer.c_customer_id, df_Customer.c_first_name, df_Customer.c_last_name, df_Item.i_item_id, df_Item.i_product_name, "Net Paid by CustomerNet Paid Incl Tax by Customer", "Net Paid Incl Tax by Customer")\
                                   .sort(df_Customer.c_first_name, df_Customer.c_last_name, df_Item.i_product_name)

display(df_SalesStoreOutput)

# COMMAND ----------

# DBTITLE 1,Display Only the Physical Plan by Not Setting "Extended Argument" to "True"
df_SalesStoreOutput.explain()

# COMMAND ----------

# DBTITLE 1,Display the Logical Plan by Not Setting "Extended Argument" to "True"
df_SalesStoreOutput.explain(True)

# The "First Section" of the "explain" Output is the "Parsed Logical Plan" or "Unresolved Logical Plan". This has "Validated" the "Code" and "Built" the "First Version" of the "Logical Plan" with the "Flow of Execution".

# The "Second Section" of the "explain" Output is the "Analyzed Logical Plan". This "Plan" has used the "Metadata Catalog" to "Validate" the "Table" or "Column" Objects, so it has now "Resolved" everything it was "Unable" to in the "Parsed Logical Plan" or "Unresolved Logical Plan".

# The "Third Section" of the "explain" Output is the "Optimized Logical Plan". As the "Analyzed Logical Plan" is "Validated" against the "Metadata Catalog" and then sent to "Catalyst Optimizer", the "Catalyst Optimizer" can "Optimize" the "Plan" based on the "Operations" it needs to "Perform".

# The "Final Section" of the "explain" Output is the "Physical Plan". Using the "Optimized Logical Plan", the "Catalyst Optimizer" has created "Multiple Physical Plans", "Compared" "Each" of the "Physical Plans" through the "Cost Model" and then "Selected" the "Best Optimal Plan" as the "Selected Physical Plan" which is "Outputted" on "Screen".

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional Parameters of "explain" Function
# MAGIC * There is an "Optional Parameter", called "mode", that can be used with the ".explain()" Function.

# COMMAND ----------

# DBTITLE 1,mode = "simple"
# This "mode" displays the "Physical Plan", like providing "No Arguments" to the ".explain()" Function.
df_SalesStoreOutput.explain(mode = "simple")

# COMMAND ----------

# DBTITLE 1,mode = "extended"
# This "mode" displays both the "Logical" and the "Physical Plan", like providing "True" Argument to the ".explain()" Function.
df_SalesStoreOutput.explain(mode = "extended")

# COMMAND ----------

# DBTITLE 1,mode = "codegen"
# This "mode" displays the "Physical Plan" and the "Generated Codes" if those are "Available".
df_SalesStoreOutput.explain(mode = "codegen")

# COMMAND ----------

# DBTITLE 1,mode = "cost"
# This "mode" displays the "Optimized Logical Plan" and the "Related Statistics", if the "Plan Node Statistics" are "Available".
df_SalesStoreOutput.explain(mode = "cost")

# COMMAND ----------

# DBTITLE 1,mode = "formatted"
# This "mode" displays "Two Sections" splitting the "Physical Plan Outline" and the "Node Details".
df_SalesStoreOutput.explain(mode = "formatted")
