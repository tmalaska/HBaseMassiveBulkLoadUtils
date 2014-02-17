# HBaseMassiveBulkLoadUtils
## Overview
This project contents a number of small executables that will add in simulating and managing HBase during a major multiple stage Bulk load process.  

These tools will:
* Help you identify how HBase will react given different configurations
* Easy to see details of every HStore files
* A more controlled compaction strategy
* Testing get performance at different levels of uncompaction

##How to Build
Simple mvn package will work

##How to execute
All command with out parameter will give you help

The main command is

hadoop jar HBaseUtilMain.jar {command}

There are a number of sub commands

* CreateTable : example of how to create a table
* GenerateBulkLoadSampleData : generates some dummy data 
* ExampleHBaseBulkLoad : An example of a bulk load
* CompactTableColumnFamily : An example of how to compact at a column family level
* CompactTable : An example of how to compact a table
* TableStats : This will give details of everything a table and its regions and store files
* GetRateTest : This will get a set of random records and test it's speed
* TestScriptGen : This will generate a simulation test
SmartTableMajorCompact : This will only compact regions with more then N Store Files


