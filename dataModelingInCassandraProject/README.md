# This Repository

 Contains the scripts and data used to accomplish the project.

# This README file

## Data Modeling in Apache Cassandra: Project Overview

 This project comprises two main sections:  

### 1. CSV Files ETL

 Data is parsed using Python's `os`, `glob` and `csv` libraries.  
 A set of CSV files is processed and consolidated into a single CSV file.


### 2. Data Ingestion and Modeling in Apache Cassandra

 The consolidated CSV is used to supply data to three differente tables,
each of these tables is modeled based on one of the three queries given for
this project.

 During the ingestion and modeling process, Cassandra's essential concepts  are exercised like:
 * Keyspaces setup;
 * Column Families ("Tables") creation procedures, statements and data types;
 * Tables are created to answer a specific query (data redundancy is OK);
 * Simple and Composite Primary Keys and Clustering Columns.
 * CQL (Cassandra Query Language) queries are used;

 Code is developed using Python's `cassandra` library in a Jupyter Notebook environment.

 ## Scripts and Files

 ### 1. dataModelingInApacheCassandra.ipynb

 Contains all of the code and can be run interactively.
