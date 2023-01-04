# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Byte"
# MAGIC * Topic: Introduction to the "Byte" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Bytes" in "Python"
# MAGIC * "<b>Bytes</b>" are "<b>Very Similar</b>" to the "<b>Strings</b>".
# MAGIC * "<b>Bytes</b>" are the "<b>Sequences</b>" of "<b>Bytes</b>".
# MAGIC * "<b>Bytes</b>" are "<b>Useful</b>" for "<b>Raw Binary Data</b>" and "<b>Fixed-Width Single-Byte Character Encoding</b>", such as, "<b>ASCII</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # How "Python Bytes" are "Created"?
# MAGIC * "<b>Literal Bytes</b>" in "<b>Python</b>" are "<b>Delimited</b>" by "<b>Quotes</b>".
# MAGIC * Both of the "<b>Single Quotes</b>" and the "<b>Double Quotes</b>" can be used.
# MAGIC * The "<b>Quote</b>" is "<b>Prefixed</b>" by a "<b>Lowercase b</b>".

# COMMAND ----------

# DBTITLE 1,Create a "Literal Byte" Using "Single Quote"
b'oindrila chakraborty'

# COMMAND ----------

# DBTITLE 1,Create a "Literal Byte" Using "Double Quote"
b"oindrila chakraborty"

# COMMAND ----------

# MAGIC %md
# MAGIC # "Operations" on "Bytes"
# MAGIC * "<b>Bytes</b>" in "<b>Python</b>" support "<b>Most</b>" of the "<b>Same Operations</b>" as "<b>String</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Indexing" of "Bytes"
# MAGIC * "<b>Indexing</b>" of "<b>Bytes</b>" "<b>Returns</b>" the "<b>Integer Value</b>" of the "<b>Byte</b>", that is "<b>Present</b>" at the "<b>Specified Index</b>".

# COMMAND ----------

# DBTITLE 1,Print the "Integer Value" of the "Third Byte", i.e., "Index-2" from a "Sequence of Bytes"
myNameByte = b'oindrila chakraborty'
print(myNameByte[2])

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Splitting" of "Bytes"
# MAGIC * The "<b>Split</b>" Operation on the "<b>Bytes</b>" "<b>Returns</b>" a "<b>List</b>" of "<b>Byte Objects</b>".
# MAGIC * The "<b>Split</b>" Operation is "<b>Performed</b>" using the "<b>split ()</b>" Method on the "<b>Sequence of Bytes Object</b>".

# COMMAND ----------

# DBTITLE 1,"Split" a "Sequence of Bytes"
myNameByte = b'oindrila chakraborty'
print(myNameByte.split())
