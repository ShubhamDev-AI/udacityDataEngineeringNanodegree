# Data Pipelining with Apache Airflow

## Intro

This project automates the ETL steps involved in a Data Warehouse batch load scenario.

To achive this, Apache Airflow is used to schedule and orchestrate the following Tasks:

1. bulk load JSON files located in Amazon S3 to staging tables in a Redshift database;
1. process raw data from staging tables and distribute it accross Fact and Dimension tables;
1. perform data quality checks right after data are loaded to its final destination;

The data are entirely processed within an AWS Redshift database instance.

The Airflow DAG ("Directed Acyclic Graph") created to coordinate these Tasks is shown below:

![Sparkify DAG](./sparkifyPipelineDAG.png?raw=true "Sparkify DAG")  
  
Notice `parallelization` also takes place to ensure faster data delivery and better organize logically-related ETL steps (parallelization requires an Airflow Executor other than `Local`).

From top to bottom, a standard Data Warehouse batch-load process happens in the following sequence:

1. Bulk Load Staging Area (raw data);
1. Load Dimensions Tables (these must come first as Fact tables may depend on it);
1. Load Fact Tables;
  
Custom Operators were created to encapsulate repetitive `DDL` and `DML` patterns into reusable Python Classes.

> NOTES: `LoadDimensionOperator` and `LoadFactOperator` perform identical tasks and so could be further consolidated into a single Operator Class.  
  
## Prerequisites

The following two conditions got to be satisfied for this project's `DAG` to run properly:

### 1. Environment Setup

This project has been developed using the `puckel/docker-airflow` Docker Image:

https://github.com/puckel/docker-airflow

With this `image` in hands, create a dedicated `container` with the following Shell command (notice Windows' `%cd%` shortcut):

`docker run -d -p 8080:8080 -v "%cd%\dags":/usr/local/airflow/dags -v "%cd%\plugins":/usr/local/airflow/plugins puckel/docker-airflow webserver`

> In addition to container creation, this command also creates two `volumes` mapping standard Airflow directories, `/dags` and `/plugins`, to HOMONYMOUS FOLDERS that MUST EXIST within the same directory `docker run` is being called.

### 2. Add Airflow `connections`

The following two Airflow Connections must be available for this DAG to run properly:
* `aws_credentials`: An Amazon Web Services type connection containing the `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` pair;
* `redshift`: a Postgres type connection pointing to an AWS Redshift database;