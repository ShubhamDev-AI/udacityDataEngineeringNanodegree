# Course Materials and Useful Links

## NoSQL Data Models

* [NoSQL Databases Overview](<https://www.xenonstack.com/blog/nosql-databases/>)

### Apache Cassandra

* [architecture tutorial](<https://www.tutorialspoint.com/cassandra/cassandra_architecture.htm/>)

* [architecture documentation](<https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/architecture/archTOC.html>)

* [data manipulation documentation](<https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/dml/dmlIntro.html>)

* [data modeling concepts](<https://docs.datastax.com/en/dse/6.7/cql/cql/ddl/dataModelingApproach.html>)

* [defining primary keys](<https://docs.datastax.com/en/archived/cql/3.3/cql/cql_using/useSimplePrimaryKeyConcept.html#useSimplePrimaryKeyConcept>)

* [compound primary keys](<https://docs.datastax.com/en/archived/cql/3.3/cql/cql_using/useCompoundPrimaryKeyConcept.html>)

* [compound primary keys usage](<https://docs.datastax.com/en/archived/cql/3.3/cql/cql_using/useCompoundPrimaryKey.html>)

* [stackoverflow on Cassandra's keys (VERY USEFUL)](<https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra>)

* [`ALLOW FILTERING` explained (and why you should avoid it)](<https://www.datastax.com/blog/2014/12/allow-filtering-explained>)

* [`ORDER BY` clauses](<https://stackoverflow.com/questions/35708118/where-and-order-by-clauses-in-cassandra-cql>)

### The CAP Theorem

CAP stands for "Consistency", "Availability" and "Partition Tolerance":

* [wikipedia article](<https://en.wikipedia.org/wiki/CAP_theorem>)

* [ACID vs CAP discussion](<https://www.voltdb.com/blog/2015/10/22/disambiguating-acid-cap/>)

## Data Warehousing

### Dimensional Modeling & OLAP Cubes

* [`ipython-sql` documentation](<https://github.com/catherinedevlin/ipython-sql>)

* [O'Reilly Dimensional Modeling Tutorial](<http://archive.oreilly.com/oreillyschool/courses/dba3/index.html>)

* [Postgres DDL Constraints Documentation](<https://www.postgresql.org/docs/12/ddl-constraints.html>)

* [Postgres COPY command documentation](<https://www.postgresql.org/docs/12/sql-copy.html>)

* [PSQL tutorial](<http://postgresguide.com/utilities/psql.html>)

* [Datacamp article on PSQL](<https://www.datacamp.com/community/tutorials/10-command-line-utilities-postgresql>)

* [PSQL official documentation](<https://www.postgresql.org/docs/12/app-psql.html>)

* [Postgres Environment Variables](<https://www.postgresql.org/docs/12/libpq-envars.html>)

* [Linux Environment Variables Setup](<https://www.serverlab.ca/tutorials/linux/administration-linux/how-to-set-environment-variables-in-linux/>)

* [Stackoverflow on PSQL credentials and scripts handling](<https://stackoverflow.com/questions/9736085/run-a-postgresql-sql-file-using-command-line-arguments>)

* [Citus' `cstore_fdw` Postgres columnar storage extension](<https://github.com/citusdata/cstore_fdw>)

### AWS Resources

* [IAM Role for Amazon Redshift setup](<https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-create-an-iam-role.html>)

* [Amazon Redshift: getting started](<https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html>)

* [Troubleshooting Redshift Connection Issues](<https://aws.amazon.com/pt/premiumsupport/knowledge-center/cannot-connect-redshift-cluster/>)

* [Virtual Private Cloud (VPC) Overview](<https://en.wikipedia.org/wiki/Virtual_private_cloud>)

* [AWS VPC Endpoints](<https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html>)

* [Saving on AWS VPC Endpoints](<https://medium.com/nubego/how-to-save-money-with-aws-vpc-endpoints-9bac8ae1319c>)

* [AWS's Python SDK: `boto3`](<https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>)

* [Python's `configparser` library documentation](<https://docs.python.org/3/library/configparser.html>)

* [Workbench/J Redshift Connection](<https://blog.openbridge.com/definitive-guide-for-connecting-sql-workbench-j-to-amazon-redshift-57d06aa32805>)

* [AWS Redshift Best Practices for Loading Data](<https://docs.aws.amazon.com/redshift/latest/dg/c_loading-data-best-practices.html>)

* [Apache AVRO](<https://avro.apache.org/>)

* [Primary Keys in AWS Redshift](<https://dev.to/naturalkey/primary-keys-in-redshift-425b>)

* [AWS Redshift Unique, Primary and Foreign Key Constraints](<https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>)  

## Data Lakes & Spark  

### Tools and Languages

* [Apache Spark Website](<http://spark.apache.org/docs/latest/index.html>)

* [Apache Flink Website](<https://flink.apache.org/>)  

* [Apache Storm Website](<http://storm.apache.org/>)  

* [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)  

* [Apache Impala](https://impala.apache.org/)

* [Pydata.org](<https://pydata.org/>)  

* [Java Virtual Machine](<https://en.wikipedia.org/wiki/Java_virtual_machine>)  

* [Lambda Calculus for Absolute Dummies](<http://palmstroem.blogspot.com/2012/05/lambda-calculus-for-absolute-dummies.html>)  

* [PySpark API Docs](<https://spark.apache.org/docs/latest/api/python/index.html>)  

* [Jupyter Docker Stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/)  

* [Jupyter Docker Stack GitHub page](https://github.com/jupyter/docker-stacks)  

* [Dive into Docker course](https://diveintodocker.com/?utm_source=nj&utm_medium=website&utm_campaign=/blog/understanding-how-the-docker-daemon-and-docker-cli-work-together)  

* [Caching Data in Spark Applications](<https://unraveldata.com/to-cache-or-not-to-cache/>)  

* [Spark API's → RDDs versus DataFrames and Datasets](<https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html>)

* [Spark Managers: Standalone vs YARN vs Mesos](<https://stackoverflow.com/questions/31806188/standalone-manager-vs-yarn-vs-mesos>)  

* [Spark Configuration Docs](<https://spark.apache.org/docs/latest/configuration.html>)  

* [Spark Performance Tuning](<https://spark.apache.org/docs/latest/tuning.html>)  

* [Spark SQL Tuning](<https://spark.apache.org/docs/latest/sql-performance-tuning.html>)

* [Spark Function Types](<https://medium.com/@mrpowers/the-different-type-of-spark-functions-custom-transformations-column-functions-udfs-bf556c9d0ce7>)

* [Spark UDF's](<https://medium.com/@mrpowers/spark-user-defined-functions-udfs-6c849e39443b>)  

* [AWS S3 Credentials usage in Spark](<http://wrschneider.github.io/2019/02/02/spark-credentials-file.html>)

* [Default AWS Environment Variables](<https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html>)