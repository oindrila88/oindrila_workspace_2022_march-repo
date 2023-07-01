# Databricks notebook source
# MAGIC %md
# MAGIC # Transaction Log of Delta Tables in Databricks
# MAGIC * Topic: Introduction to "Transaction Logs" of "Delta Table".
# MAGIC * Author: Oindrila Chakraborty

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Delta Lake Transaction Log"?
# MAGIC * The "Delta Lake Transaction Log" (also known as the "DeltaLog") is an "Ordered Record" of Every "Transaction" that has ever been Performed on a "Delta Table" since its "Creation".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Transaction Log" As a "Single Source of Truth"
# MAGIC * "Delta Lake" is "Built" on top of "Apache Spark" in "Order" to allow "Multiple Readers" and "Writers" of a given "Delta Table" to "All" Work on the "Same Delta Table" at the "Same Time".
# MAGIC * In order to "Show" the Users the "Correct Views" of the "Data" at "All Times", the "Delta Lake Transaction Log" Serves as a "Single Source of Truth", i.e., the "Central Repository" that "Tracks" "All" the "Changes" that Users make to the corresponding "Delta Table".
# MAGIC * When a User "Reads" a "Delta Table" for the "First Time" or "Runs" a "New Query" on an "Open Table" that has been "Modified" since the "Last Time" it was read, "Apache Spark" "Checks" the "Transaction Log" to "See" what "New Transactions" have "Posted" to the "Delta Table", and then "Updates" the "End User’s" "Delta Table" with those "New Changes". This ensures that a User’s Version of a "Delta Table" is always "Synchronized" with the "Master Record" as of the "Most Recent Query", and that Users "Cannot" make "Divergent", "Conflicting Changes" to a "Delta Table".

# COMMAND ----------

# MAGIC %md
# MAGIC # "Implementing" the "Atomicity" on the "Delta Lake" Using the "Transaction Log"
# MAGIC * One of the "Four Properties" of "ACID Transactions" is the "Atomicity", which Guarantees that "Operations" (like an "INSERT" or "UPDATE") Performed on the "Data Lake" either "Complete Fully", or "Don’t Complete" At All. Without this Property, it’s far too easy for a "Hardware Failure" or a "Software Bug" to "Cause" the "Data" to be Only "Partially Written" to a "Delta Table", resulting in "Messy" or "Corrupted Data".
# MAGIC * The "Transaction Log" is the "Mechanism" through which "Delta Lake" is able to Offer the Guarantee of "Atomicity". For All Intents and Purposes, if it’s "Not Recorded" in the "Transaction Log", it "Never Happened". By Only "Recording" the "Transactions" that "Execute Fully" and "Completely", and Using that "Record" as the "Single Source of Truth", the "Delta Lake Transaction Log" allows the Users to reason about the "Data", and have peace of mind about its fundamental trustworthiness, at petabyte scale.

# COMMAND ----------

# MAGIC %md
# MAGIC # Breaking Down "Transactions" Into "Atomic Commits"
# MAGIC * Whenever a User performs an "Operation" to "Modify" a "Delta Table" (such as an "INSERT", "UPDATE" or "DELETE"), "Delta Lake" breaks that "Operation" down into a "Series" of Discrete Steps composed of one or more of the "Actions" below -
# MAGIC * A. Add File – Adds a "Data File".
# MAGIC * B. Remove File – "Removes" a "Data File".
# MAGIC * C. Update Metadata – "Updates" the "Table’s Metadata" (e.g., "Changing" the "Table’s Name", "Schema" or "Partitioning").
# MAGIC * D. Set Transaction – "Records" that a "Structured Streaming Job" has "Committed" a "Micro-Batch" with the given "ID".
# MAGIC * E. Change Protocol – "Enables" New Features by "Switching" the "Delta Lake Transaction Log" to the "Newest Software Protocol".
# MAGIC * F. Commit Info – Contains Information around the "Commit", which "Operation" was made, from "Where" and at "What Time".
# MAGIC * Those "Actions" are then "Recorded" in the "Transaction Log" as "Ordered", "Atomic Units" known as "Commits".
# MAGIC * Example - Suppose a User Creates a "Transaction" to "Add" a "New Column" to a "Delta Table" plus "Add" Some more "Data" to it. "Delta Lake" would break that "Transaction" down into its "Component Parts", and once the "Transaction" Completes, "Add" those to the "Transaction Log" as the following "Commits" -
# MAGIC * A. Update Metadata – "Change" the "Schema" to include the "New Column".
# MAGIC * B. Add File – For Each New File Added.

# COMMAND ----------

# DBTITLE 1,Display the Values Stored in a "Commit" JSON File
dbutils.fs.head("/user/hive/warehouse/retailer_db.db/tbl_managedcustomeraddress/_delta_log/00000000000000000000.json")

# COMMAND ----------

# DBTITLE 1,Display the Values Stored in a "crc" File
dbutils.fs.ls("/user/hive/warehouse/retailer_db.db/tbl_managedcustomeraddress/_delta_log/00000000000000000000.crc")

# COMMAND ----------

# MAGIC %md
# MAGIC # The "Delta Lake Transaction Log" at the "File Level"
# MAGIC * When a User Creates a "Delta Table", that Table’s "Transaction Log" is "Automatically Created" in the "_delta_log" Sub-Directory.
# MAGIC * As the User makes "Changes" to that "Delta Table", those "Changes" are "Recorded" as "Ordered", "Atomic Commits" in the "Transaction Log".
# MAGIC * "Each Commit" is "Written" out as a "JSON File", starting with "000000.json". "Additional Changes" to the "Delta Table" Generate the "Subsequent JSON Files" in "Ascending Numerical Order" so that the "Next Commit" is "Written" out as "000001.json", the following as "000002.json", and so on.
# MAGIC * <img src = '/files/tables/images/delta_1.jpeg'>
# MAGIC * Example - A "Delta Table" is "Created" from the "Data Files" - "1.parquet" and "2.parquet". That "Transaction" would "Automatically" be "Added" to the "Transaction Log", "Saved" to the "Disk" after "Commit" as "000000.json". Then, the User decides to "Remove" those "Data Files" and add a new "Data File" instead ("3.parquet"). Those "Actions" would be "Recorded "as the "Next Commit" in the "Transaction Log", as "000001.json".
# MAGIC * <img src = '/files/tables/images/delta_2.jpeg'>
# MAGIC * Even though "1.parquet" and "2.parquet" are "No Longer" part of the "Delta Table", the "Addition" and "Removal" of those "Data Files" are still "Recorded" in the "Transaction Log" because those "Operations" were performed on the "Delta Table",  despite the fact that those "Operations" ultimately canceled each other out. "Delta Lake" still "Retains" the "Atomic Commits" like these to ensure that in the event to "Audit" the concerned "Delta Table" or to Use "Time Travel" to "See" "What" the "Delta Table" looked like at a given point in time, it can be done so "Accurately".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Checkpoint File"
# MAGIC * Once a Total of "10 Commits" are made to the "Transaction Log", the "Delta Lake" Saves a "Checkpoint File" in "Parquet" Format in the same "_delta_log" Sub-Directory. "Delta Lake" Automatically "Generates" the "Checkpoint Files" after "Every 10 Commits".
# MAGIC * <img src = 'files/tables/images/delta_3.jpeg'>
# MAGIC * These "Checkpoint Files" Save the Entire "State" of the "Delta Table" at a point in Time, in Native "Parquet" Format that is Quick and Easy for "Spark" to Read. In other words, the "Checkpoint Files" Offer the "Spark Reader" a sort of "Shortcut" to "Fully Reproducing" a "Delta Table’s" "State" that allows "Spark" to Avoid the "Reprocessing" "What" could be "Thousands of Tiny", "Inefficient JSON Files".
# MAGIC * To get up to speed, "Spark" can "Run" a "listFrom" Operation to "View" All the "Files" in the "Transaction Log", "Quickly Skip" to the "Newest Checkpoint File", and only "Process" those "JSON Commits" that are made "Since" the "Most Recent Checkpoint File" was "Saved".
# MAGIC * Example - Imagine that there are "JSON Commits" all the way through "000007.json". "Spark" is up to speed through this commit, having "Automatically Cached" the "Most Recent Version" of the concerned "Delta Table" in "Memory". In the meantime, though, Several other "Writers" have "Written" the "New Data" to the concerned "Delta Table", "Adding Commits" all the way through "0000012.json".
# MAGIC * To incorporate these "New Transactions" and "Update" the "State" of the "Delta Table", "Spark" will then "Run" a "listFrom version 7" Operation to "See" the "New Changes" to the "Delta Table".
# MAGIC * <img src = 'files/tables/images/delta_4.jpeg'>
# MAGIC * Rather than "Processing" All of the "Intermediate JSON Files", "Spark" can "Skip" ahead to the "Most Recent Checkpoint File", since it contains the Entire "State" of the "Delta Table" at "Commit #10". Now, "Spark" only has to Perform the "Incremental Processing" of "0000011.json" and "0000012.json" to have the "Current State" of the "Delta Table". "Spark" then "Caches" the "Version 12" of the concerned "Delta Table" in "Memory". By following this workflow, "Delta Lake" is able to Use "Spark" to Keep the "State" of a "Delta Table" that is "Updated" at All Times in an "Efficient Manner".

# COMMAND ----------

# MAGIC %md
# MAGIC # What is "Optimistic Concurrency Control"?
# MAGIC * "Optimistic Concurrency Control" is a "Method" of Dealing with "Concurrent Transactions" that Assumes that "Transactions" ("Changes") made to a "Delta Table" by different Users can "Complete" without "Conflicting" with "One Another".
# MAGIC * It is "Incredibly Fast" because when Dealing with Petabytes of "Data", there’s a "High Likelihood" that Users will be Working on different Parts of the "Data" altogether, allowing to "Complete" the "Non-Conflicting Transactions" Simultaneously.
# MAGIC * Example - Imagine that "UserA" and "UserB" are Working on a "Jigsaw Puzzle" Together. As long as they are both Working on Different Parts of it, there is no problem. It’s only when "UserA" and "UserB" need the "Same Pieces", at the "Same Time", that there’s a "Conflict". That’s "Optimistic Concurrency Control".
# MAGIC * Of course, even with "Optimistic Concurrency Control", sometimes Users do try to "Modify" the "Same Parts" of the "Data" at the "Same Time". Luckily, "Delta Lake" has a "Protocol" for that.

# COMMAND ----------

# MAGIC %md
# MAGIC # Solving Conflicts "Optimistically"
# MAGIC * In order to Offer the "ACID Transactions", the "Delta Lake" has a Protocol for Figuring Out How "Commits" should be "Ordered" (known as the Concept of "Serializability" in "Databases"), and "Determining" What to do in the "Event" that "Two" or "More Commits" are made at the "Same Time".
# MAGIC * "Delta Lake" handles these cases by "Implementing" a "Rule 'of "Mutual Exclusion", then "Attempting" to "Solve" any "Conflict Optimistically".
# MAGIC * This "Protocol" allows "Delta Lake" to "Deliver" on the "ACID principle" of "Isolation", which ensures that the resulting "State" of the "Delta Table" after "Multiple", "Concurrent Writes" is the "Same" as if those "Writes" had "Occurred Serially", in "Isolation from One Another".
# MAGIC * In general, the process proceeds like this -
# MAGIC * A. "Record" the "Starting Table Version"
# MAGIC * B. "Record" the "Reads"/"Writes".
# MAGIC * C. "Attempt" a "Commit".
# MAGIC * D. "Check" whether anything being "Read" has "Changed".
# MAGIC * E. "Repeat".
# MAGIC * To "See" how the "Delta Lake" manages "Conflicts" when the "Conflicts" Pop up. Imagine that "Two Users" "Read" from the "Same Table", then each go about "Attempting" to "Add" some "Data" to it.
# MAGIC * <img src = 'files/tables/images/delta_5.jpeg'>
# MAGIC * "Delta Lake" "Records" the "Starting Table Version" of the "Delta Table" ("Version 0") that is "Read" prior to making any "Changes".
# MAGIC * "User 1" and "User 2" both "Attempt" to "Append" some "Data" to the "Delta Table" at the "Same Time". Here, a "Conflict" will "Occur" because Only "One Commit" can "Come Next" and be "Recorded" as "000001.json".
# MAGIC * "Delta Lake" handles this "Conflict" with the Concept of "Mutual Exclusion", which means that Only "One User" can Successfully make "Commit" for "000001.json". "User 1’s" "Commit" is "Accepted", while "User 2’s "is "Commit" is "Rejected".
# MAGIC * Rather than Throwing an "Exception" for "User 2", "Delta Lake" prefers to handle this "Conflict Optimistically". It Checks to "See" whether any "New Commits" have been made to the "Delta Table", and "Updates" the "Delta Table" Silently to "Reflect" those "Changes", then simply "Retries" "User 2’s" "Commit" on the "Newly Updated Delta Table" (without any Data Processing), "Successfully Committing" the "000002.json".
# MAGIC * In the vast majority of cases, this "Reconciliation" happens "Silently", "Seamlessly", and "Successfully". However, in the Event that there’s an "irreconcilable Problem" that "Delta Lake" cannot solve "Optimistically" (for example, if "User 1" has Deleted a File that "User 2" has also Deleted), the only "Option" is to Throw an "Exception".
# MAGIC * As a final note, since "All" of the "Transactions" made on "Delta Tables" are "Stored Directly" to "Disk", this Process Satisfies the "ACID Property" of "Durability", meaning it will "Persist" Even in the Event of "System Failure".
