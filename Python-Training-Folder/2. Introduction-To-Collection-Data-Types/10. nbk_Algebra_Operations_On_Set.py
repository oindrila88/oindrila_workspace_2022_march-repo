# Databricks notebook source
# MAGIC %md
# MAGIC # "Algebra Operations" On "Set"
# MAGIC * Topic: Different "Algebra Operations" that Can be "Performed" on the "Set" Collection Data Type
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # "Algebra Operations" on "Set"
# MAGIC * Perhaps, the most useful aspect of the "<b>Set</b>" Collection Data Type is the "<b>Group of Powerful Set Algebra Operations</b>".
# MAGIC * The following different "<b>Set Methods</b>" are used to "<b>Compute</b>" the corresponding "<b>Algebra Operations</b>" on the "<b>Sets</b>" -
# MAGIC <br><b>1</b>. <b>Set Union</b> - The "<b>Union Operation</b>" is "<b>Performed</b>" through the "<b>union ()</b>" Method.
# MAGIC <br><b>2</b>. <b>Set Intersection</b> - The "<b>Intersection Operation</b>" is "<b>Performed</b>" through the "<b>intersection ()</b>" Method.
# MAGIC <br><b>3</b>. <b>Set Difference</b> - The "<b>Difference Operation</b>" is "<b>Performed</b>" through the "<b>difference ()</b>" Method.
# MAGIC <br><b>4</b>. <b>Set Symmetric Difference</b> - The "<b>Symmetric Difference Operation</b>" is "<b>Performed</b>" through the "<b>symmetric_difference ()</b>" Method.
# MAGIC * The following "<b>Three</b>" "<b>Set Predicate Methods</b>" <b>Evaluate</b>" the "<b>Relationships</b>" between "<b>Two Sets</b>" -
# MAGIC <br><b>1</b>. <b>Subset Relationship</b> - The "<b>Subset Relationship</b>" is "<b>Checked</b>" through the "<b>issubset ()</b>" Method.
# MAGIC <br><b>2</b>. <b>Superset Relationship</b> - The "<b>Superset Relationship</b>" is "<b>Checked</b>" through the "<b>issuperset ()</b>" Method.
# MAGIC <br><b>3</b>. <b>Disjoint Set Relationship</b> - The "<b>Disjoint Set Relationship</b>" is "<b>Checked</b>" through the "<b>isdisjoint ()</b>" Method.

# COMMAND ----------

# MAGIC %md
# MAGIC ## "union ()" Method
# MAGIC * The "<b>union ()</b>" Method "<b>Collects Together</b>" the "<b>Elements</b>", which are in "<b>Either</b>", or, "<b>Both</b>" of the "<b>Sets</b>".
# MAGIC <br><img src = '/files/tables/images/union_set.jpg'>

# COMMAND ----------

# DBTITLE 1,"Find" the "People" Who "Live" In "Kolkata" or "Bangalore" Using "union ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.union(peopleLivesInBangaloreSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>union ()</b>" Method is a "<b>Commutative Operation</b>".
# MAGIC * It is possible to "<b>Swap</b>" the "<b>Order</b>" of the "<b>Operands</b>" using the "<b>Value</b>" of the "<b>Equality Operator</b>" to "<b>Check</b>" for the "<b>Equivalence</b>" of the "<b>Resulting Sets</b>".

# COMMAND ----------

# DBTITLE 1,"Demonstrate" that "union ()" Method is a "Commutative Operation"
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.union(peopleLivesInBangaloreSet) == peopleLivesInBangaloreSet.union(peopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## "intersection ()" Method
# MAGIC * The "<b>intersection ()</b>" Method "<b>Collects Only</b>" the "<b>Elements</b>", which are "<b>Present</b>" in "<b>Both</b>" of the "<b>Sets</b>".
# MAGIC <br><img src = '/files/tables/images/intersection_set.jpg'>

# COMMAND ----------

# DBTITLE 1,"Find" the "People" Who "Live" In "Both" of the "Cities", i.e., "Kolkata" and "Bangalore" Using "intersection ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.intersection(peopleLivesInBangaloreSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>intersection ()</b>" Method is also a "<b>Commutative Operation</b>".
# MAGIC * It is possible to "<b>Swap</b>" the "<b>Order</b>" of the "<b>Operands</b>" using the "<b>Value</b>" of the "<b>Equality Operator</b>" to "<b>Check</b>" for the "<b>Equivalence</b>" of the "<b>Resulting Sets</b>".

# COMMAND ----------

# DBTITLE 1,"Demonstrate" that "intersection ()" Method is a "Commutative Operation"
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.intersection(peopleLivesInBangaloreSet) == peopleLivesInBangaloreSet.intersection(peopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## "difference ()" Method
# MAGIC * The "<b>difference ()</b>" Method "<b>Collects All</b>" the "<b>Elements</b>", which is "<b>Present</b>" in the "<b>First Set</b>", i.e., The "<b>Set</b>" on which the "<b>Difference</b>" Method is "<b>Called</b>", and, "<b>Not Present</b>" in the "<b>Second Set</b>".
# MAGIC <br><img src = '/files/tables/images/difference_set.jpg'>

# COMMAND ----------

# DBTITLE 1,"Find" the "People" Who "Live Only" In "Kolkata" and "Not" in "Bangalore" Using "difference ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.difference(peopleLivesInBangaloreSet)

# COMMAND ----------

# DBTITLE 1,"Find" the "People" Who "Live Only" In "Bangalore" and "Not" in "Kolkata" Using "difference ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInBangaloreSet.difference(peopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>difference ()</b>" Method is a "<b>Non-Commutative Operation</b>".

# COMMAND ----------

# DBTITLE 1,"Demonstrate" that "difference ()" Method is a "Non-Commutative Operation"
bengaliPeopleLivesInKolkataSet.difference(bengaliPeopleLivesInBangaloreSet) == bengaliPeopleLivesInBangaloreSet.difference(bengaliPeopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## "symmetric_difference ()" Method
# MAGIC * The "<b>symmetric_difference ()</b>" Method "<b>Collects All</b>" the "<b>Elements</b>", which are "<b>Present</b>" in the "<b>First Set</b>", or, in the "<b>Second Set</b>", but, "<b>Not Present</b>" in "<b>Both</b>" of the "<b>Sets</b>".
# MAGIC <br><img src = '/files/tables/images/symmetric_difference.jpg'>

# COMMAND ----------

# DBTITLE 1,"Find" the "People" Who "Live" "Only" In "Bangalore" and "Only" In "Kolkata", but, "Not in Both" Using "symmetric_difference ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInBangaloreSet.symmetric_difference(peopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC * The "<b>symmetric_difference ()</b>" Method is a "<b>Commutative Operation</b>".
# MAGIC * It is possible to "<b>Swap</b>" the "<b>Order</b>" of the "<b>Operands</b>" using the "<b>Value</b>" of the "<b>Equality Operator</b>" to "<b>Check</b>" for the "<b>Equivalence</b>" of the "<b>Resulting Sets</b>".

# COMMAND ----------

# DBTITLE 1,"Demonstrate" that "symmetric_difference ()" Method is a "Commutative Operation"
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInBangaloreSet = {"Avishek", "Avirupa", "Rohan", "Debarshi", "Ayan"}

peopleLivesInKolkataSet.symmetric_difference(peopleLivesInBangaloreSet) == peopleLivesInBangaloreSet.symmetric_difference(peopleLivesInKolkataSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## "issubset ()" Method
# MAGIC * The "<b>issubset ()</b>" Method "<b>Checks</b>" whether "<b>All</b>" the "<b>Elements</b>" of the "<b>Set</b>", on which the "<b>issubset ()</b>" Method is "<b>Called</b>", are "<b>Present</b>" in the "<b>Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>issubset ()</b>" Method.
# MAGIC * That means, the "<b>issubset ()</b>" Method "<b>Checks</b>" whether the "<b>Set</b>", on which the "<b>issubset ()</b>" Method is "<b>Called</b>", is a "<b>Subset</b>" of "<b>Another Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>issubset ()</b>" Method.
# MAGIC <br><img src = '/files/tables/images/subset.jpg'>

# COMMAND ----------

# DBTITLE 1,"Check" If the "People" Who "Live" In "Kolkata", Also "Live" In "India" Using "issubset ()" Method
peopleLivesInIndiaSet = {"Oindrila", "Soumyajyoti", "Rohan", "Avishek", "Avirupa", "Debarshi", "Ayan"}
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}

print(peopleLivesInKolkataSet.issubset(peopleLivesInIndiaSet))

# COMMAND ----------

# MAGIC %md
# MAGIC ## "issuperset ()" Method
# MAGIC * The "<b>issuperset ()</b>" Method "<b>Checks</b>" whether "<b>All</b>" the "<b>Elements</b>" of the "<b>Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>issuperset ()</b>" Method, are "<b>Present</b>" in the "<b>Set</b>", on which the "<b>issuperset ()</b>" Method is "<b>Called</b>".
# MAGIC * That means, the "<b>issuperset ()</b>" Method "<b>Checks</b>" whether the "<b>Set</b>", on which the "<b>issuperset ()</b>" Method is "<b>Called</b>", is a "<b>Superset</b>" of "<b>Another Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>issuperset ()</b>" Method.
# MAGIC <br><img src = '/files/tables/images/superset.jpg'>

# COMMAND ----------

# DBTITLE 1,"Check" If the "People" Who "Live" In "India", Also "Live" In "Kolkata" Using "issuperset ()" Method
peopleLivesInIndiaSet = {"Oindrila", "Soumyajyoti", "Rohan", "Avishek", "Avirupa", "Debarshi", "Ayan"}
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}

print(peopleLivesInIndiaSet.issuperset(peopleLivesInKolkataSet))

# COMMAND ----------

# MAGIC %md
# MAGIC ## "isdisjoint ()" Method
# MAGIC * The "<b>isdisjoint ()</b>" Method "<b>Checks</b>" whether "<b>None</b>" of the "<b>Elements</b>" of the "<b>Set</b>", on which the "<b>isdisjoint ()</b>" Method is "<b>Called</b>", are "<b>Present</b>" in the "<b>Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>isdisjoint ()</b>" Method.
# MAGIC * That means, the "<b>isdisjoint ()</b>" Method "<b>Checks</b>" whether the "<b>Set</b>", on which the "<b>isdisjoint ()</b>" Method is "<b>Called</b>", has "<b>No Element</b>" in "<b>Common</b>" with "<b>Another Set</b>", which is "<b>Passed</b>" as the "<b>Argument</b>" to the "<b>isdisjoint ()</b>" Method.
# MAGIC <br><img src = '/files/tables/images/disjoint_sets.jpg'>

# COMMAND ----------

# DBTITLE 1,"Check" If the "People" Who "Live" In "Kolkata", "Do Not Live" In "Delhi" Using "isdisjoint ()" Method
peopleLivesInKolkataSet = {"Oindrila", "Soumyajyoti", "Rohan"}
peopleLivesInDelhiSet = {"Ayanti", "Arghya", "Ria", "Souvik"}

print(peopleLivesInKolkataSet..isdisjoint(peopleLivesInDelhiSet))
