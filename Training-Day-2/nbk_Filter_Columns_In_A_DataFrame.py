# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Training Day - 2
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Filter a Single or Multiple Columns from that DataFrame in Different Scenarios.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# DBTITLE 1,Read a CSV File Using "csv" method of "DataFrameReader" and Create a DataFrame
df_ReadCustomerFileUsingCsv = spark.read\
                                    .option("header", "true")\
                                    .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")
display(df_ReadCustomerFileUsingCsv)

# COMMAND ----------

# DBTITLE 1,Filter All Rows Using "where ()" and "filter ()" Methods of DataFrame - Example of Bad Practice
# Filter All Rows Where "c_birth_day", "c_birth_month", and "c_birth_year" are Invalid - Bad Practice
from pyspark.sql.functions import col

df_FilterCustomerOnInvalidBirthDateBadPractice = df_ReadCustomerFileUsingCsv\
                              .filter((col("c_birth_day").isNotNull()) & (col("c_birth_day") > 0) & (col("c_birth_day") <= 31))\
                              .where((col("c_birth_month").isNotNull()) & (col("c_birth_month") > 0) & (col("c_birth_month") <= 12))\
                              .filter((col("c_birth_year").isNotNull()) & (col("c_birth_year") > 0))
display(df_FilterCustomerOnInvalidBirthDateBadPractice)

# COMMAND ----------

# DBTITLE 1,Filter All Rows Using "where ()" and "filter ()" Methods of DataFrame - Example of Good Practice
# Filter All Rows Where "c_birth_day", "c_birth_month", and "c_birth_year" are Invalid - Good Practice
from pyspark.sql.functions import col

df_FilterCustomerOnInvalidBirthDateGoodPractice = df_ReadCustomerFileUsingCsv\
                                                                              .filter(col("c_birth_day").isNotNull())\
                                                                              .where(col("c_birth_day") > 0)\
                                                                              .filter(col("c_birth_day") <= 31)\
                                                                              .where(col("c_birth_month").isNotNull())\
                                                                              .filter(col("c_birth_month") > 0)\
                                                                              .where(col("c_birth_month") <= 12)\
                                                                              .filter(col("c_birth_year").isNotNull())\
                                                                              .where(col("c_birth_year") > 0)
display(df_FilterCustomerOnInvalidBirthDateGoodPractice)

# COMMAND ----------

# DBTITLE 1,Filter All Rows Where Birth Month, i.e., "c_birth_month" is "January"
from pyspark.sql.functions import col

df_FilterCustomerWithJanuaryBirthMonth = df_ReadCustomerFileUsingCsv\
                                                              .filter(col("c_birth_month").isNotNull())\
                                                              .where(col("c_birth_month") == 1)
display(df_FilterCustomerWithJanuaryBirthMonth)

# COMMAND ----------

# DBTITLE 1,Filter All Rows Where Birth Month, i.e., "c_birth_month" is Any Other Month But "January"
from pyspark.sql.functions import col

df_FilterCustomerWithNotJanuaryBirthMonth = df_ReadCustomerFileUsingCsv\
                                                                  .filter(col("c_birth_month").isNotNull())\
                                                                  .where(col("c_birth_month") != 1)
display(df_FilterCustomerWithNotJanuaryBirthMonth)

# COMMAND ----------

# DBTITLE 1,Filter All the Rows Where "c_birth_country" Starts With "J" Using "startswith ()" Method
from pyspark.sql.functions import col, trim

df_FilterCustomerByBirthCountryStartsWithJ = df_ReadCustomerFileUsingCsv\
                                                                    .filter(trim(col("c_birth_country")).startswith("J"))
display(df_FilterCustomerByBirthCountryStartsWithJ)

# COMMAND ----------

# DBTITLE 1,Filter All the Rows Where "c_birth_country" Ends With "S" Using "endswith ()" Method
from pyspark.sql.functions import col

df_FilterCustomerByBirthCountryEndsWithS = df_ReadCustomerFileUsingCsv\
                                                                    .filter(trim(col("c_birth_country")).endswith("S"))
display(df_FilterCustomerByBirthCountryEndsWithS)

# COMMAND ----------

# DBTITLE 1,Filter All the Rows Where "c_birth_country" Contains the Value "J" Using "contains ()" Method
from pyspark.sql.functions import col

df_FilterCustomerByBirthCountryContainingJ = df_ReadCustomerFileUsingCsv\
                                                                    .filter(trim(col("c_birth_country")).contains("J"))
display(df_FilterCustomerByBirthCountryContainingJ)

# COMMAND ----------

# DBTITLE 1,Filter All the Rows Where "c_birth_country" Contains the Value Like "%JIKI%" Using "like ()" Method
from pyspark.sql.functions import col

df_FilterCustomerByBirthCountryContainingJIKI = df_ReadCustomerFileUsingCsv\
                                                                    .filter(trim(col("c_birth_country")).like("%JIKI%"))
display(df_FilterCustomerByBirthCountryContainingJIKI)

# COMMAND ----------

# DBTITLE 1,Filter and Display All the Rows Where "c_first_name" Have NULL Values Using "isNull ()" Method
from pyspark.sql.functions import col

df_FilterCustomerByFirstNameWithNull = df_ReadCustomerFileUsingCsv\
                                                                .filter(col("c_first_name").isNull())
display(df_FilterCustomerByFirstNameWithNull)

# COMMAND ----------

# DBTITLE 1,Filter and Display All the Rows Where "c_last_name" Have Any Values, But, NULL Using "isNotNull ()" Method
from pyspark.sql.functions import col

df_FilterCustomerByLastNameWithoutNull = df_ReadCustomerFileUsingCsv\
                                                                .filter(col("c_last_name").isNotNull())
display(df_FilterCustomerByLastNameWithoutNull)

# COMMAND ----------

# DBTITLE 1,Filter and Display All the Rows Where the Values of "c_customer_sk" is Between "100" and "1000"
from pyspark.sql.functions import col

df_FilterCustomerByCustomerSkBetweenValues = df_ReadCustomerFileUsingCsv\
                                                                    .filter(col("c_customer_sk").between(100, 1000))
display(df_FilterCustomerByCustomerSkBetweenValues)

# COMMAND ----------

# DBTITLE 1,Filter Rows Using "SQL Expression" in "filter" Function
from pyspark.sql.functions import col, trim

df_FilterCustomerUsingSqlExpression = df_ReadCustomerFileUsingCsv\
                                                                .filter("trim(c_first_name) == 'Brian' and trim(c_last_name) == 'Taylor'")
display(df_FilterCustomerUsingSqlExpression)

# COMMAND ----------

# DBTITLE 1,Display All Rows Where Column Values are Equal to the List of Elements
from pyspark.sql.functions import col, lower

listOfCountries = ['Afghanistan', 'Gabon', 'Japan']

# Use "List Comprehension" to "Create" Another "List" from an "Existing List".
lowerListOfCountries = [country.lower() for country in listOfCountries]

#Select All Customers Having Birth Country as "Afghanistan", "Gabon", or, "Japan".
df_FilterCustomerUsingBirthCountryPresentInList = df_ReadCustomerFileUsingCsv\
                                                                            .filter(lower(col("c_birth_country")).isin(lowerListOfCountries))
display(df_FilterCustomerUsingBirthCountryPresentInList)

# COMMAND ----------

# DBTITLE 1,Display All Rows Where Column Values are Not Equal to the List of Elements
from pyspark.sql.functions import col, lower

listOfFirstNames = ['David', 'Ronnie', 'Samuel', 'Kathleen']

#Select All Customers Not Having First Names as "David", "Ronnie", "Samuel" or, "Kathleen".
df_FilterCustomerUsingFirstNameNotPresentInList = df_ReadCustomerFileUsingCsv\
                                                                            .filter(~col("c_first_name").isin(listOfFirstNames))
display(df_FilterCustomerUsingBirthCountryPresentInList)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame in Ascending Order Using "sort ()" Method
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInAsc = df_ReadCustomerFileUsingCsv\
                                                    .sort(col("c_first_name").asc())
display(df_SortCustomerByFirstNameInAsc)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame in Descending Order Using "sort ()" Method
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInDesc = df_ReadCustomerFileUsingCsv\
                                                            .sort(col("c_first_name").desc())
display(df_SortCustomerByFirstNameInDesc)

# COMMAND ----------

# DBTITLE 1,Sort Multiple Columns of a DataFrame Using "sort ()" Method in Both in Ascending And Descending Order
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInAscLastNameInDesc = df_ReadCustomerFileUsingCsv\
                                                                        .sort(col("c_first_name").asc(), col("c_last_name").desc())
display(df_SortCustomerByFirstNameInAscLastNameInDesc)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame Using "sort ()" and "expr ()" Methods in Descending Order
from pyspark.sql.functions import col, expr

df_SortCustomerByFirstNameInDesc = df_ReadCustomerFileUsingCsv\
                                                            .filter(col("c_first_name").isNotNull())\
                                                            .sort(expr("c_first_name").desc())
display(df_SortCustomerByFirstNameInDesc)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame in Ascending Order Using "orderBy ()" Method
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInAsc = df_ReadCustomerFileUsingCsv\
                                                    .orderBy(col("c_first_name").asc())
display(df_SortCustomerByFirstNameInAsc)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame in Descending Order Using "orderBy ()" Method
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInDesc = df_ReadCustomerFileUsingCsv\
                                                            .orderBy(col("c_first_name").desc())
display(df_SortCustomerByFirstNameInDesc)

# COMMAND ----------

# DBTITLE 1,Sort Multiple Columns of a DataFrame Using "orderBy ()" Method in Both in Ascending And Descending Order
from pyspark.sql.functions import col

df_SortCustomerByFirstNameInAscLastNameInDesc = df_ReadCustomerFileUsingCsv\
                                                                        .orderBy(col("c_first_name").asc(), col("c_last_name").desc())
display(df_SortCustomerByFirstNameInAscLastNameInDesc)

# COMMAND ----------

# DBTITLE 1,Sort One Column of a DataFrame Using "orderBy ()" and "expr ()" Methods in Descending Order
from pyspark.sql.functions import expr

df_SortCustomerByFirstNameInDesc = df_ReadCustomerFileUsingCsv\
                                                            .orderBy(expr("c_first_name").desc())
display(df_SortCustomerByFirstNameInDesc)
