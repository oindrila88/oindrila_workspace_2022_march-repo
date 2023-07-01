# Databricks notebook source
# MAGIC %md
# MAGIC # Use Window Functions on DataFrame Using PySpark in Databricks
# MAGIC * Topic: Read CSV File and Load the Data into a DataFrame. Then Use the Window Functions on that DataFrame.
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What are Window Functions?
# MAGIC * "<b>Window Functions</b>" are useful when the "<b>Relationships</b>" within the "<b>Groups of Data</b>" are "<b>Examined</b>", rather than "<b>Between</b>" the "<b>Groups of Data</b>" (as for "<b>groupBy</b>").
# MAGIC * At its core, a "<b>Window Function</b> calculates a "<b>Return Value</b>" for "<b>Every Input Row</b>" of a "<b>Table</b>", based on a "<b>Group of Rows</b>", called the "<b>Frame</b>".
# MAGIC * "<b>Every Input Row</b>" can have a "<b>Unique Frame</b>" associated with it.
# MAGIC * There are mainly "<b>Three</b>" Types of "<b>Window Function</b>" -
# MAGIC <br>1. <b>Analytical Function</b>
# MAGIC <br>2. <b>Ranking Function</b>
# MAGIC <br>3. <b>Aggregate Function</b>

# COMMAND ----------

# MAGIC %md
# MAGIC # Working Techniques of Window Function
# MAGIC * To use "<b>Window Functions</b>", it is required to "Mark" that a "<b>Function</b>" is used as a "<b>Window Function</b>" by "<b>Calling</b>" the "<b>over</b>" Method on a "Supported Function" in the "<b>DataFrame API</b>", e.g. "<b>rank().over(...)</b>".
# MAGIC * Once a "<b>Function</b>" is "<b>Marked</b>" as a "<b>Window Function</b>", the "Next Key Step" is to define the "<b>Window Specification</b>" associated with the "<b>Window Function</b>".
# MAGIC * A "<b>Window Specification</b>" defines "<b>Which Rows</b>" are "<b>Included</b>" in the "<b>Frame</b>" associated with a given "<b>Input Row</b>".
# MAGIC * A "<b>Window Specification</b>" includes "<b>Three</b>" parts -
# MAGIC <br>1. <b>Partitioning Specification</b>: The <b>Partitioning Specification</b> controls "<b>Which Rows</b>" will be in the "<b>Same Partition</b>" with the "<b>Given Row</b>.
# MAGIC <br>Also, it might be required that "<b>All the Rows</b>" having the <b>Same Value</b>" for the "<b>Category Column</b>" are "<b>Collected</b>" to the "<b>Same Machine</b>" before "<b>Ordering</b>" and "<b>Calculating</b>" the "<b>Frame</b>".
# MAGIC <br>If "<b>No Partitioning Specification</b>" is "<b>Given</b>", then "<b>All the Data</b>" must be "<b>Collected</b>" to a "<b>Single Machine</b>".
# MAGIC <br><br><b>Ordering Specification</b>: The <b>Ordering Specification</b> controls the "<b>Way</b>" the "<b>Rows</b>" in a "<b>Partition</b>" are "<b>Ordered</b>", which "Determines</b>" the "<b>Position</b>" of the "<b>Given Row</b>" in its "<b>Partition</b>".
# MAGIC <br><br>3. <b>Frame Specification</b>: The <b>Frame Specification</b> states "<b>Which Rows</b> will be "<b>Included</b>" in the "<b>Frame</b>" for the "<b>Current Input Row</b>", based on the "<b>Relative Position</b>" to the "<b>Current Row</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How to Use Window Function Using DataFrame API?
# MAGIC * To "<b>Perform</b>" the "<b>Window Function</b>" Operation on a "<b>Group of Rows</b>", "<b>First</b>", a "<b>Partition</b>" needs to be created, i.e. a "<b>Group of Data Rows</b>" needs to be defined by using the "<b>window.partition()</b>" Function. This is the "<b>Partitioning Specification</b>".
# MAGIC * For the "<b>Ranking Function</b>", it is required to additionally implement "<b>Order By</b>" on the "<b>Partition Data</b>" using the "<b>orderBy</b>" Function. This is the <b>Ordering Specification</b>.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ranking Function
# MAGIC * The "<b>Ranking Function</b>" returns the "<b>Statistical Rank</b>" of a "<b>Given Value</b>" for "<b>Each Row</b>" in a "<b>Partition</b>" or "<b>Group</b>".
# MAGIC * The "<b>Goal</b>" of the "<b>Ranking Functions</b>" is to provide "<b>Consecutive Numbering</b>" in the "<b>Resultant Column</b>" of the "<b>Rows</b>", set by the "<b>Order</b>", selected in the "<b>Window.partition</b>" for "<b>Each Partition</b>" specified in the "<b>over</b>" Function.

# COMMAND ----------

# MAGIC %md
# MAGIC # "row_number" Ranking Window Function
# MAGIC * The "<b>row_number()</b>" Window Function is "<b>Used</b>" to "<b>Give</b>" a "<b>Sequential Number</b>" to "<b>Each Row</b>" "<b>Starting</b>" from "<b>1</b>" to the "<b>End</b>" of "<b>Each Window Partition</b>" in the "<b>Table</b>".

# COMMAND ----------

# DBTITLE 1,Find the "Section" with the "Third Highest Scorer" in the "Entire Class" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class").orderBy(col("Marks").desc())

df_Student_MarksWithRowNumber = df_Student_Marks\
                                                 .withColumn("row_number", row_number().over(windowSpec))

display(df_Student_MarksWithRowNumber)

display(df_Student_MarksWithRowNumber.filter("row_number == 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "rank" Ranking Window Function
# MAGIC * The "<b>rank()</b>" Window Function is "<b>Used</b>" to "<b>Give</b>" the "<b>Ranks</b>" to the "<b>Rows</b>", specified in the "<b>Window Partition</b>".
# MAGIC * The "<b>rank()</b>" Window Function "<b>Leaves</b>" the "<b>Gaps</b>" in the "<b>Rank</b>" if there are "<b>Ties</b>".

# COMMAND ----------

# DBTITLE 1,Find the "Section" with the "Fourth Highest Scorer" in the "Entire Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class").orderBy(col("Marks").desc())

df_Student_MarksWithRank = df_Student_Marks\
                                           .withColumn("rank", rank().over(windowSpec))

display(df_Student_MarksWithRank)

display(df_Student_MarksWithRank.filter("rank == 4"))

# COMMAND ----------

# DBTITLE 1,Find the "Second Highest Scorer" in "Each Section" of "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section").orderBy(col("Marks").desc())

df_Student_MarksWithRank = df_Student_Marks\
                                           .withColumn("rank", rank().over(windowSpec))

display(df_Student_MarksWithRank)

display(df_Student_MarksWithRank.filter("rank == 2"))

# COMMAND ----------

# MAGIC %md
# MAGIC # "dense_rank" Ranking Window Function
# MAGIC * The "<b>dense_rank()</b>" Window Function is "<b>Used</b>" to "<b>Give</b>" the "<b>Ranks</b>" to the "<b>Rows</b>", specified in the "<b>Window Partition</b>" in the "<b>Form</b>" of "<b>Row Numbers</b>".
# MAGIC * The "<b>dense_rank()</b>" Window Function is "<b>Similar</b>" to the "<b>rank()</b>" Window Function, except that the "<b>rank()</b>" Window Function "<b>Leaves Gaps</b>" in the "<b>Rank</b>" when there are "<b>Ties</b>".

# COMMAND ----------

# DBTITLE 1,Find the "Section" with the "Fourth Highest Scorer" in the "Entire Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class").orderBy(col("Marks").desc())

df_Student_MarksWithDenseRank = df_Student_Marks\
                                                .withColumn("dense_rank", dense_rank().over(windowSpec))

display(df_Student_MarksWithDenseRank)

display(df_Student_MarksWithDenseRank.filter("dense_rank == 4"))

# COMMAND ----------

# DBTITLE 1,Find the "Second Highest Scorer" in "Each Section" of "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section").orderBy(col("Marks").desc())

df_Student_MarksWithDenseRank = df_Student_Marks\
                                                .withColumn("dense_rank", dense_rank().over(windowSpec))

display(df_Student_MarksWithDenseRank)

display(df_Student_MarksWithDenseRank.filter("dense_rank == 2"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregate Function
# MAGIC * "<b>Aggregate Functions</b>", such as "SUM" or "MAX", "<b>Operate</b>" on a "<b>Group of Rows</b>" and "<b>Calculate</b>" a "<b>Single Return Value</b>" for "<b>Every Group</b>".
# MAGIC * For the "<b>Aggregate Functions</b>", it is "<b>Not Required</b>" to "<b>Order By</b>" on the "<b>Partition Data</b>" using the "<b>orderBy</b>" Function.

# COMMAND ----------

# DBTITLE 1,Find the "Highest Scorer" of "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, when

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentWithHighestScorer = df_Student_Marks\
                                               .withColumn("Is Highest Scorer", when(max(col("Marks")).over(windowSpec) == col("Marks"), "Yes").otherwise("No"))

display(df_StudentWithHighestScorer)

# COMMAND ----------

# DBTITLE 1,Find the "Lowest Scorer" of "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, when

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentWithLowestScorer = df_Student_Marks\
                                               .withColumn("Is Lowest Scorer", when(min(col("Marks")).over(windowSpec) == col("Marks"), "Yes").otherwise("No"))

display(df_StudentWithLowestScorer)

# COMMAND ----------

# DBTITLE 1,Find the "Total Number of Students" Present in "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentCountInSection = df_Student_Marks\
                                           .withColumn("Total Students In Section", count(col("Roll")).over(windowSpec))

display(df_StudentCountInSection)

# COMMAND ----------

# DBTITLE 1,Find the "Average Score" of "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, when

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentWithAverageScore = df_Student_Marks\
                                             .withColumn("Is Average Scorer", when(avg(col("Marks")).over(windowSpec) == col("Marks"), "Yes").otherwise("No"))

display(df_StudentWithAverageScore)

# COMMAND ----------

# DBTITLE 1,Find the "Total Marks" Obtained by "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentTotalMarksInSection = df_Student_Marks\
                                               .withColumn("Total Marks In Section", sum(col("Marks")).over(windowSpec))

display(df_StudentTotalMarksInSection)

# COMMAND ----------

# DBTITLE 1,Find How Much "Percentage" of Marks "Each Student" Scored "Against" the "Total Marks" of "Each Section" in "Each Class"
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, round

df_Student_Marks = spark.read\
                        .option("header", "true")\
                        .csv("/FileStore/tables/retailer/data/Students_Marks.csv")

windowSpec = Window.partitionBy("Class", "Section")

df_StudentMarkAsPercentageOfTotal = df_Student_Marks\
                                                    .withColumn("Total Marks Per Section", sum(col("Marks")).over(windowSpec))\
                                                    .withColumn("% of Total Marks", round((col("Marks") * 100)/col("Total Marks Per Section")))

display(df_StudentMarkAsPercentageOfTotal)
