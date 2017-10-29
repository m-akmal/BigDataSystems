# SparkStructuredStreaming
SparkStructuredStreaming

Files:
Q1: PartAQuestion1.scala - Maintains state for hourly window updated every 30mins
Q2: PartAQuestion2.scala - Maintains user mentions updated every 10sec
Q3: PartAQuestion3.scala - Maintains user tweet actions updated every 5sec for users mentioned in a HDFS file

CustomConsoleSink: Implemented custom extended version of console sink to print all of the streaming state

stream-emulator.sh: Script to move a csv file from source to stream dir in HDFS every 5sec

All the jobs disable Spark Logging on console.

Steps to build code:
sbt clean package
