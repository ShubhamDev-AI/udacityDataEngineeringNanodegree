
# This Repository

 Contains the studies resulting from Udacity's Data Engineering Nanodegree.

# This README File

 Provides an overview of the projects tackled during the course.

## Project 1 - Data Modeling with PostgreSQL

![PostgreSQL Logo](postgresLogo.png)

 A PostgreSQL database is setup and five tables are created in it.
The tables are Dimensionally modeled into a Star Schema, comprising
Fact and Dimension tables describing songs and user activity from a
Music Streaming App.

 The data for these tables lies in multiple directories of JSON files,
upon which is then applied an ETL process in Python for later ingestion.  
 Code is developed and tested in Jupyter Notebooks and then "rolled out"
to production as python scripts.

## Project 2 - Data Modeling with Apache Cassandra

![Apache Cassandra Logo](cassandraLogo.png)

 A Keyspace is setup in Apache Cassandra. Three Tables (Cassandra's "Column
Families") are then created, their modeling based on Three queries regarding
user activity in a Music Streaming App.

 The data for these tables is located in a series of CSV files, which are
then consolidated into a single file by an ETL process and then ingested into
Cassandra's tables. Coding is done in a Jypyter Notebook.
