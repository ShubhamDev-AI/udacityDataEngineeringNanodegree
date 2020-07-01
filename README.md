
# This Repository

 Contains the studies resulting from Udacity's Data Engineering Nanodegree.

# This README File

 Provides an overview of the projects tackled during the course.

## Project 1 - Data Modeling with PostgreSQL

![PostgreSQL Logo](./postgresLogo.png)

 A PostgreSQL database is setup and five tables are created in it.
The tables are Dimensionally modeled into a Star Schema, comprising
Fact and Dimension tables describing songs and user activity from a
Music Streaming App.

 The data for these tables are in multiple directories of JSON files,
upon which is then applied an ETL process in Python for later ingestion.  
 Code is developed and tested in Jupyter Notebooks and then "rolled out"
to production as python scripts.

## Project 2 - Data Modeling with Apache Cassandra

![Apache Cassandra Logo](./cassandraLogo.png)

 A Keyspace is setup in Apache Cassandra. Three tables (Cassandra's "Column
Families") are then created, their modeling based on three queries regarding
user activity in a Music Streaming App.

 The data for these tables are located in a series of CSV files, which are
then consolidated into a single file by an ETL process and then ingested into
Cassandra's tables. Coding is done in a Jypyter Notebook.

## Project 3 - Cloud Data Warehousing with AWS Redshift

![Amazon Web Services Logo](./AWSLogo.png)  
  
 A Data Warehouse is created using an AWS Redshift instance. The cluster is programatically setup using python's `boto3` library. Database modeling and loading leverages various Redshift concepts like Distribution Keys, Sort Keys and Parallel Bulk Loads.  
   
 The data used in this project reside in a series of S3 Buckets, which are read and consolidated into two staging tables within the database. Finally, there's a Jupyter Notebook with data exploration results leveraging the Dimensional data model resulting from this project.

## Project 4 - Data Lakes with Spark  

![Spark Logo](<./apacheSparkLogo.jpg>)

## Project 5 - Data Pipelining with Apache Airflow  

![Airflow Logo](<./airflow_logo.png>)