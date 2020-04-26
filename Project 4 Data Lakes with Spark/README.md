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

In spite of dealing entirely with small JSON files, this project proved to be an invaluable learning experience of Spark's `PySpark` API and how it compares to manipulating data with SQL.  
  
It was very interesting to provision an AWS EMR Cluster, SSH into it, execute the project and watch how performance could scale by adding more worker nodes. But a good deal of Linux Command Line had to be learned prior to it for things to start falling into place so I simultaneously resorted to an additional Linux course in another MOOC platform.  
  
Additionally I'd like to share three experiences that caught my attention during development:
  
### Table Partitioning  

A 96% reduction in execution time is possible by choosing to save the data to S3 without any partitioning strategy.  
  
Smaller but still significant gains have also been achieved by applying the same strategy to other two S3 Write operations as evidenced by "Table 1" below.

### Avoid Redundant Ingestion and ETL  

 A 99% reduction in data ingestion time has been achieved by choosing not to load the entire "song_data" S3 directory one more time during the `process_log_data()` etl function call.

 Two non-partitioned Parquet files having only the necessary data were loaded instead: `dim_songs_non_partitioned` and `dim_artists`. This provided the following benefits:  

 * Much smaller I/O overhead;
 * Avoidance of repetitive tranformation tasks thus making code simpler;  

### Know Your Data: Always Declare Data Schemas Before Ingestion  

Overall ETL process execution time may be reduced by approximately 55% when the ingested JSON data schema is explicitly declared by assigning Spark's `StructType()` objects to them.  
  
Performance is significantly affected when the system is left "to guess" the ingested data schema implicitly.

![Execution Comparisons](<etlScriptExecutionAnalysis.PNG>)  

\* Data for the 3 Tables above can be found within the `projectSubmissionScriptExecutionLog.txt` file available in this repository. The executions starting at 2020-04-19 16:33:02 UTC and 2020-04-21 12:22:28 were used in this comparison.