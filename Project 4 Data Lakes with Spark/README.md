# Data Lakes and Apache Spark  
  
![Apache Spark Logo](<apacheSparkLogo.png>)  

## Intro

This project reads data from Udacity's Data Engineering S3 Bucket, processes it using Apache Spark and then saves the resulting dimensionally modeled data into another S3 Bucket, ready for analysis and consumption.  

This ETL pipeline has been developed for the fictional `Sparkify` music streaming App and its purpose is to create a Data Lake environment with the same entities created during the previous Data Warehousing assignment. This time though data is entirely processed using the `PySpark` API for Apache Spark's Framework. The script is then deployed and executed in Amazon's `EMR` service.
  
This repository contains the scripts developed for this project and some analyses and lessons learned during development.  
  
##  Executing the Scripts
  
### 1. `dl.cfg`

Store your AWS Credentials in this file. 
It'll be read by the `etl.py` script to access the necessary S3 Buckets.  
`dl.cfg` must have the following layout:

>[AWS_CREDENTIALS]  
>AWS_ACCESS_KEY_ID=yourAwsAccessKeyWithNoSingleNorDoubleQuotes  
>AWS_SECRET_ACCESS_KEY=yourAwsSecretAccessKeyWithNoSingleNorDoubleQuotes  

### 2. `etl.py`  
  
The main project script, it is responsible for the entire ETL pipeline execution. It imports many `PySpark SQL` module classes, and other standard python libraries required for its execution.  
  
## Conclusions  
  
### Table Partitioning  

### Avoiding Redundant Ingestion and ETL  

### Know Your Data: Always Declare its Schema before Ingestion  



![Execution Comparisons](<etlScriptExecutionAnalysis.PNG>)