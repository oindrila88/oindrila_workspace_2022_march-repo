# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 2
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Perform Different Aggregate Operations on that DataFrame.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .option("inferSchema", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Count Total Number of Records in a DataFrame Using "count ()" Method of "DataFrame"
df_ReadCustomerFileUsingCsv.count()

# COMMAND ----------

# DBTITLE 1,Count Total Number of Records in a DataFrame Using "count ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import count

df_ReadCustomerFileUsingCsv.select(count("*")).show()

# COMMAND ----------

# DBTITLE 1,Count Total Number of Records of Multiple Columns in a DataFrame Using "count ()" Function of "pyspark.sql.functions" Package
df_ReadCustomerFileUsingCsv.select(count("c_first_name"), count("c_last_name")).show()

# COMMAND ----------

# DBTITLE 1,Count Total Number of Distinct Records of a Column in a DataFrame Using "countDistinct ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import countDistinct

df_ReadCustomerFileUsingCsv.select(countDistinct("c_first_name")).show()

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadItem = spark.read\
                   .options(\
                               header = "true",\
                               delimiter = "|",\
                               inferSchema = "true"\
                           )\
                   .csv("dbfs:/FileStore/tables/retailer/data/item.dat")
display(df_ReadItem)

# COMMAND ----------

# DBTITLE 1,Find Minimum Value of Multiple Columns of a DataFrame Using "min ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import col, min

df_ReadItem.select(min("i_current_price").alias("min_current_price"), min("i_wholesale_cost").alias("min_wholesale_cost")).show()

# COMMAND ----------

# DBTITLE 1,Find Maximum Value of Multiple Columns of a DataFrame Using "max ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import col, max

df_ReadItem.select(max("i_current_price").alias("max_current_price"), max("i_wholesale_cost").alias("max_wholesale_cost")).show()

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadSalesStore = spark.read\
                         .options(\
                                   header = "true",\
                                   sep = "|",\
                                   inferSchema = "true"\
                                 )\
                         .csv("dbfs:/FileStore/tables/retailer/data/store_sales.dat")
display(df_ReadSalesStore)

# COMMAND ----------

# DBTITLE 1,Sum All the Values of Multiple Columns of a DataFrame Using "sum ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import sum

display(df_ReadSalesStore.select(sum("ss_net_paid_inc_tax"), sum("ss_net_profit")))

# COMMAND ----------

# DBTITLE 1,Sum All the Distinct Values of Multiple Columns of a DataFrame Using "sum_distinct ()" Function of "pyspark.sql.functions" Package
from pyspark.sql.functions import sum, sum_distinct

display(df_ReadSalesStore.select(sum_distinct("ss_net_paid_inc_tax"), sum_distinct("ss_net_profit")))

# COMMAND ----------

# DBTITLE 1,Calculate the Average Value of a Column of a DataFrame Using "avg ()" and "mean ()" Functions of "pyspark.sql.functions" Package
from pyspark.sql.functions import avg, mean, sum, count

display(df_ReadSalesStore.select(\
                                   avg("ss_quantity").alias("average_purchase"),\
                                   mean("ss_quantity").alias("mean_purchase"),\
                                   (sum("ss_quantity") / count("ss_quantity")).alias("average_quantity")\
                                )\
       )

# COMMAND ----------

# DBTITLE 1,Use Multiple Aggregate Functions in "agg ()" Method When Grouping By Using One / More Columns of a DataFrame
from pyspark.sql.functions import countDistinct, sum, max, min, avg

df_GroupSalesStoreByCustomerId = df_ReadSalesStore.groupBy("ss_customer_sk")\
                                                  .agg(\
                                                        countDistinct("ss_item_sk").alias("ItemCount"),\
                                                        sum("ss_quantity").alias("TotalQuantity"),\
                                                        sum("ss_net_paid").alias("TotalNetPaid"),\
                                                        max("ss_net_paid").alias("MaxPaidPerItem"),\
                                                        min("ss_net_paid").alias("MinPaidPerItem"),\
                                                        avg("ss_net_paid").alias("AveragePaidPerItem")\
                                                      )\
                                                  .withColumnRenamed("ss_customer_sk", "CustomerID")\
                                                  .show()
