# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction To "Python"
# MAGIC * Topic: Introduction to the "Python" Programming Language
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # Why to Learn "Python"
# MAGIC * Among the many reasons, following are the "<b>Main Reasons</b>" to "<b>Learn Python</b>" -
# MAGIC * <b>1</b>. <b>Python is Versatile</b> - "<b>Python</b>" can be used for a "<b>Wide Variety of Applications</b>", like - "<b>Data Science</b>", "<b>Machine Learning</b>", "<b>Web Development</b>", and, more.
# MAGIC <br><img src = '/files/tables/images/Why_Python_1.jpg'>
# MAGIC * <b>2</b>. <b>Strong Python Community</b> - There is a "<b>Strong Python Community</b>", which means that there is a "<b>Package</b>" for pretty much "<b>Anything</b>". So, there is "<b>No Need</b>" to "<b>Re-Invent</b>" the "<b>Wheel</b>".
# MAGIC <br><img src = '/files/tables/images/Why_Python_2.jpg'>
# MAGIC * <b>3</b>. <b>Easy to Learn</b> - "<b>Python</b>" is "<b>Easy to Learn</b>". It is "<b>Written Similar</b>" to "<b>English</b>", or, a "<b>Spoken Language</b>", and, it is "<b>Concise</b>".
# MAGIC <br>"<b>Python</b>" is an "<b>Interpreted Language</b>". So, it "<b>Does Not Need</b>" to be "<b>Compiled First</b>", and, then "<b>Run</b>".
# MAGIC <br><img src = '/files/tables/images/Why_Python_3.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # Where to Write "Python Code"
# MAGIC * There are "<b>Two Ways</b>" to "<b>Write</b>" the "<b>Python Code</b>" -
# MAGIC   * <b>1</b>. <b>Using</b> "<b>Python Interpreter</b>"
# MAGIC   * <b>2</b>. <b>Using</b> "<b>IDE</b>"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write "Python Code" Using "Python Interpreter"
# MAGIC * In the "<b>Command Line Interface</b>", "<b>After Typing</b>" "<b>py</b>", or, "<b>python</b>" and "<b>Pressing Enter</b>", the "<b>Python Interpreter</b>" is "<b>Started</b>".
# MAGIC * "<b>After</b>" the "<b>Three Carets</b>", i.e., "<b>>>></b>", the "<b>Lines of Code</b>" in "<b>Python</b>" can be "<b>Entered</b>", and, the "<b>Result</b>" can be "<b>Displayed Immediately</b>".
# MAGIC <br><img src = '/files/tables/images/Where_Python_Code_Runs_1.jpg'>
# MAGIC * It is also possible to "<b>Use</b>" the "<b>Python Interpreter</b>" to "<b>Assign Values</b>" to "<b>Variables</b>".
# MAGIC <br>Example - "<b>Display</b>" the "<b>Full Name</b>" of a "<b>Person</b>".
# MAGIC   * "<b>First</b>", "<b>Create</b>" a "<b>Variable</b>", called "<b>firstName</b>" by "<b>Assigning</b>" the "<b>Value</b>", i.e., "<b>Oindrila</b>" to it.
# MAGIC   * "<b>Second</b>", "<b>Create</b>" another "<b>Variable</b>", called "<b>lastName</b>" by "<b>Assigning</b>" the "<b>Value</b>", i.e., "<b>Chakraborty</b>" to it.
# MAGIC   * "<b>Third</b>", "<b>Create</b>" the "<b>Final Variable</b>", called "<b>fullName</b>" by "<b>Concatenating</b>" the previous "<b>Two Variables</b>", i.e., "<b>firstName</b>", and, "<b>lastName</b>".
# MAGIC * Then, in the "<b>Python Interpreter</b>", if "<b>Only</b>" the "<b>Name</b>" of the "<b>Final Variable</b>" is "<b>Entered</b>", and, "<b>Enter</b>" is "<b>Pressed</b>", the "<b>Value</b>" of the "<b>Final Variable</b>", i.e., "<b>OindrilaChakraborty</b>" is "<b>Displayed</b>".
# MAGIC <br><img src = '/files/tables/images/Where_Python_Code_Runs_2.jpg'>

# COMMAND ----------

# MAGIC %md
# MAGIC # What Happens When a "Variable" is "Created"?
# MAGIC * When a "<b>Variable</b>" is "<b>Created</b>", a "<b>Piece of Memory</b>" is "<b>Created</b>", and, "<b>Labeled</b>" for it on the "<b>Computer</b>" the "<b>Code</b>" is "<b>Running</b>".
# MAGIC * If a "<b>Variable</b>", called "<b>firstName</b>", is "<b>Created</b>" with the "<b>Value</b>" as "<b>Oindrila</b>", then there is a "<b>Piece of Memory</b>", that is "<b>labelled</b>" as "<b>firstName</b>", and, currently "<b>Stores</b>" the "<b>Value</b>" as "<b>Oindrila</b>".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Python Variables" Don't Take "Data Types" in the "Variable Declaration"
# MAGIC * In some "<b>Programming Languages</b>", it is "<b>Mandatory</b>" to "<b>Define</b>" the "<b>Data Type</b>" of the "<b>Variable</b>".
# MAGIC * "<b>Python</b>" "<b>Infers</b>" the "<b>Data Type</b>" of the "<b>Variable</b>".
# MAGIC * Example -
# MAGIC   * If a "<b>Variable</b>", called "<b>amount</b>", is "<b>Created</b>" with the "<b>Value</b>" as "<b>1000</b>", "<b>Python</b>" "<b>Makes</b>" the "<b>Data Type</b>" of the "<b>Variable</b>" "<b>amount</b>" as "<b>int</b>", because, the "<b>Value</b>" is a "<b>Whole Number</b>".
# MAGIC   <br><img src = '/files/tables/images/Var_1.jpg'>
# MAGIC   * If a "<b>Variable</b>", called "<b>firstName</b>", is "<b>Created</b>" with the "<b>Value</b>" as "<b>Oindrila</b>", "<b>Python</b>" "<b>Makes</b>" the "<b>Data Type</b>" of the "<b>Variable</b>" "<b>firstName</b>" as "<b>str</b>", because, the "<b>Value</b>" is a "<b>String</b>".
# MAGIC   <br><img src = '/files/tables/images/Var_2.jpg'>
# MAGIC   * If a "<b>Variable</b>", called "<b>amount</b>", is "<b>Created</b>" with the "<b>Value</b>" as "<b>1000.50</b>", "<b>Python</b>" "<b>Makes</b>" the "<b>Data Type</b>" of the "<b>Variable</b>" "<b>amount</b>" as "<b>float</b>", because, the "<b>Value</b>" is a "<b>Decimal Number</b>".
# MAGIC   <br><img src = '/files/tables/images/Var_3.jpg'>
