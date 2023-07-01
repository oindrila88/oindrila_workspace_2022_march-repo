# Databricks notebook source
# MAGIC %md
# MAGIC # Generate the "Execution Plan" Using "Sprak SQL" in Databricks
# MAGIC * Topic: Introduction to "Execution Plan"
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate "Execution Plan"
# MAGIC * Both the "<b>Logical</b> and <b>Physical Plans</b>" can be "<b>Generated</b>" using "<b>Spark SQL</b>" by using the "<b>EXPLAIN</b>" Clause.
# MAGIC * By default, when "<b>No Parameter</b>" is "<b>Supplied</b>" with the "<b>EXPLAIN</b>" Clause, it Returns the "<b>Physical Plan</b>".

# COMMAND ----------

# DBTITLE 1,Create a View on "customer.csv" File by Reading it as a DataFrame
df_Customer = spark.read\
                        .option("header", "true")\
                        .option("inferSchema", "true")\
                        .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

display(df_Customer)

df_Customer.createOrReplaceTempView("v_Customer")

# COMMAND ----------

# DBTITLE 1,Create a View on "item.dat" File by Reading it as a DataFrame
df_Item = spark.read\
               .options(\
                         header = "true",\
                         delimiter = "|",\
                         inferSchema = "true"\
                       )\
               .csv("dbfs:/FileStore/tables/retailer/data/item.dat")

display(df_Item)

df_Item.createOrReplaceTempView("v_Item")

# COMMAND ----------

# DBTITLE 1,Create a View on "store_sales.dat" File by Reading it as a DataFrame
df_SalesStore = spark.read\
                     .options(\
                               header = "true",\
                               sep = "|",\
                               inferSchema = "true"\
                             )\
                     .csv("dbfs:/FileStore/tables/retailer/data/store_sales.dat")

display(df_SalesStore)

df_SalesStore.createOrReplaceTempView("v_SalesStore")

# COMMAND ----------

# DBTITLE 1,Execute a Query
# MAGIC %sql
# MAGIC SELECT             C.c_customer_id,
# MAGIC                    C.c_first_name,
# MAGIC                    C.c_last_name,
# MAGIC                    I.i_item_id,
# MAGIC                    I.i_product_name,
# MAGIC                    (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                    (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM               v_SalesStore S
# MAGIC INNER JOIN         v_Customer C
# MAGIC ON                 S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN         v_Item I
# MAGIC ON                 S.ss_item_sk = I.i_item_sk
# MAGIC WHERE              C.c_birth_country LIKE 'J%'
# MAGIC                    AND C.c_first_name IS NOT NULL
# MAGIC                    AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY           C.c_first_name,
# MAGIC                    C.c_last_name,
# MAGIC                    I.i_product_name;

# COMMAND ----------

# DBTITLE 1,Display Only the Physical Plan by Not Providing Any "Parameter" to the "EXPLAIN" Clause
# MAGIC %sql
# MAGIC EXPLAIN SELECT             C.c_customer_id,
# MAGIC                            C.c_first_name,
# MAGIC                            C.c_last_name,
# MAGIC                            I.i_item_id,
# MAGIC                            I.i_product_name,
# MAGIC                            (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                            (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM                       v_SalesStore S
# MAGIC INNER JOIN                 v_Customer C
# MAGIC ON                         S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN                 v_Item I
# MAGIC ON                         S.ss_item_sk = I.i_item_sk
# MAGIC WHERE                      C.c_birth_country LIKE 'J%'
# MAGIC                            AND C.c_first_name IS NOT NULL
# MAGIC                            AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY                   C.c_first_name,
# MAGIC                            C.c_last_name,
# MAGIC                            I.i_product_name;

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional Parameters of "EXPLAIN" Clause
# MAGIC * It is possible to provide "Optional Parameter" that can be used with the "EXPLAIN" Clause.

# COMMAND ----------

# DBTITLE 1,"EXTENDED" Parameter of "EXPLAIN" Clause
# MAGIC %sql
# MAGIC EXPLAIN EXTENDED SELECT             C.c_customer_id,
# MAGIC                                     C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_item_id,
# MAGIC                                     I.i_product_name,
# MAGIC                                     (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                                     (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM                                v_SalesStore S
# MAGIC INNER JOIN                          v_Customer C
# MAGIC ON                                  S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN                          v_Item I
# MAGIC ON                                  S.ss_item_sk = I.i_item_sk
# MAGIC WHERE                               C.c_birth_country LIKE 'J%'
# MAGIC                                     AND C.c_first_name IS NOT NULL
# MAGIC                                     AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY                            C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_product_name;
# MAGIC
# MAGIC -- If the "Extended" Parameter is "Supplied" with the "EXPLAIN" Clause, it Returns the "Logical Plan", showing the "Parsed Logical Plan", "Analyzed Logical Plan", "Optimized Logical Plan" and "Physical Plan".
# MAGIC
# MAGIC -- The "First Section" of the "EXPLAIN" Clause Output is the "Parsed Logical Plan" or "Unresolved Logical Plan". This has "Validated" the "Code" and "Built" the "First Version" of the "Logical Plan" with the "Flow of Execution".
# MAGIC
# MAGIC -- The "Second Section" of the "EXPLAIN" Clause Output is the "Analyzed Logical Plan". This "Plan" has used the "Metadata Catalog" to "Validate" the "Table" or "Column" Objects, so it has now "Resolved" everything it was "Unable" to in the "Parsed Logical Plan" or "Unresolved Logical Plan".
# MAGIC
# MAGIC -- The "Third Section" of the "EXPLAIN" Clause Output is the "Optimized Logical Plan". As the "Analyzed Logical Plan" is "Validated" against the "Metadata Catalog" and then sent to "Catalyst Optimizer", the "Catalyst Optimizer" can "Optimize" the "Plan" based on the "Operations" it needs to "Perform".
# MAGIC
# MAGIC -- The "Final Section" of the "EXPLAIN" Clause Output is the "Physical Plan". Using the "Optimized Logical Plan", the "Catalyst Optimizer" has created "Multiple Physical Plans", "Compared" "Each" of the "Physical Plans" through the "Cost Model" and then "Selected" the "Best Optimal Plan" as the "Selected Physical Plan" which is "Outputted" on "Screen".

# COMMAND ----------

# DBTITLE 1,"CODEGEN" Parameter of "EXPLAIN" Clause
# MAGIC %sql
# MAGIC EXPLAIN CODEGEN SELECT              C.c_customer_id,
# MAGIC                                     C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_item_id,
# MAGIC                                     I.i_product_name,
# MAGIC                                     (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                                     (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM                                v_SalesStore S
# MAGIC INNER JOIN                          v_Customer C
# MAGIC ON                                  S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN                          v_Item I
# MAGIC ON                                  S.ss_item_sk = I.i_item_sk
# MAGIC WHERE                               C.c_birth_country LIKE 'J%'
# MAGIC                                     AND C.c_first_name IS NOT NULL
# MAGIC                                     AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY                            C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_product_name;
# MAGIC
# MAGIC -- If the "CODEGEN" Parameter is "Supplied" with the "EXPLAIN" Clause, it Returns the  the "Physical Plan" and the "Generated Codes" if those are "Available".

# COMMAND ----------

# DBTITLE 1,"COST" Parameter of "EXPLAIN" Clause
# MAGIC %sql
# MAGIC EXPLAIN COST SELECT                 C.c_customer_id,
# MAGIC                                     C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_item_id,
# MAGIC                                     I.i_product_name,
# MAGIC                                     (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                                     (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM                                v_SalesStore S
# MAGIC INNER JOIN                          v_Customer C
# MAGIC ON                                  S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN                          v_Item I
# MAGIC ON                                  S.ss_item_sk = I.i_item_sk
# MAGIC WHERE                               C.c_birth_country LIKE 'J%'
# MAGIC                                     AND C.c_first_name IS NOT NULL
# MAGIC                                     AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY                            C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_product_name;
# MAGIC
# MAGIC -- If the "COST" Parameter is "Supplied" with the "EXPLAIN" Clause, it Returns the "Optimized Logical Plan" and the "Related Statistics", if the "Plan Node Statistics" are "Available".

# COMMAND ----------

# DBTITLE 1,"FORMATTED" Parameter of "EXPLAIN" Clause
# MAGIC %sql
# MAGIC EXPLAIN FORMATTED SELECT            C.c_customer_id,
# MAGIC                                     C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_item_id,
# MAGIC                                     I.i_product_name,
# MAGIC                                     (SUM(CAST(S.ss_net_paid AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid by Customer`,
# MAGIC                                     (SUM(CAST(S.ss_net_paid_inc_tax AS INT)) OVER (PARTITION BY S.ss_customer_sk)) AS `Net Paid Incl Tax by Customer`
# MAGIC FROM                                v_SalesStore S
# MAGIC INNER JOIN                          v_Customer C
# MAGIC ON                                  S.ss_customer_sk = C.c_customer_sk
# MAGIC INNER JOIN                          v_Item I
# MAGIC ON                                  S.ss_item_sk = I.i_item_sk
# MAGIC WHERE                               C.c_birth_country LIKE 'J%'
# MAGIC                                     AND C.c_first_name IS NOT NULL
# MAGIC                                     AND C.c_last_name IS NOT NULL
# MAGIC ORDER BY                            C.c_first_name,
# MAGIC                                     C.c_last_name,
# MAGIC                                     I.i_product_name;
# MAGIC
# MAGIC -- If the "FORMATTED" Parameter is "Supplied" with the "EXPLAIN" Clause, it Returns "Two Sections" splitting the "Physical Plan Outline" and the "Node Details".
