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

* [What is Amazon Redshift?](<https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html>)

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

* [AWS EMR FAQ's](<https://aws.amazon.com/emr/faqs/>)

* [EMR Notebooks: Install Libraries](<https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/>)  

* [RDD Transformations and Actions explaned](<https://trongkhoanguyen.com/spark/understand-rdd-operations-transformations-and-actions/>)

* [Spark Concepts Overview](<http://queirozf.com/entries/spark-concepts-overview-clusters-jobs-stages-tasks-etc>)

* [Spark Maximum Recommended Task Size](<https://stackoverflow.com/questions/28878654/spark-using-python-how-to-resolve-stage-x-contains-a-task-of-very-large-size-x?rq=1>)

* [Spark SQL StructType](<https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-StructType.html>)  

* [Add StructType Columns to Spark DataFrames](<https://medium.com/@mrpowers/adding-structtype-columns-to-spark-dataframes-b44125409803>)  
  
## Data Pipelines & Apache Airflow

### Complimentary Reads  

* [ETL vs ELT](<https://www.quora.com/When-is-ELT-preferred-over-ETL-seems-that-all-can-be-done-in-ETL>)  

* [Apache Kafka Intro](<https://kafka.apache.org/intro>)  

* [Airflow Documentation](<https://airflow.apache.org/docs/stable/>)  
  
* [**Airflow Python API Reference**](<https://airflow.apache.org/docs/stable/_api/index.html>)  

* [Airflow Tutorial](<https://airflow.apache.org/docs/stable/tutorial.html>)
  
* [Airflow's `DAG()` Class Docs](<https://airflow.apache.org/docs/stable/_api/airflow/models/dag/index.html>)  
  
* [](<>)  
  
* [](<>)  
  
* [](<>)  
  
## General Materials

### Python Programming  

* [Scope of Variables in Python](<https://www.datacamp.com/community/tutorials/scope-of-variables-python>)  

* [Google Docstring Style](<https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>)

* [DataCamp Python Functions Tutorial](<https://www.datacamp.com/community/tutorials/functions-python-tutorial>)

* [`*args` and `**kwargs` in Python](<https://realpython.com/python-kwargs-and-args/>)

* [Iterate through Python Dictionaries](<https://realpython.com/iterate-through-dictionary-python/>)  

* [How Apache Hudi Works](<https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi-how-it-works.html>)  

* [List S3 Bucket Contents with Boto3](<https://stackoverflow.com/questions/35803027/retrieving-subfolders-names-in-s3-bucket-from-boto3>)

* [Python's `logging` library](<https://docs.python.org/3/library/logging.html>)  

* [Basic `logging` library tutorial](<https://docs.python.org/3/howto/logging.html#logging-basic-tutorial>)  

* [`functools` library: functions and operations on callable objects ](<https://docs.python.org/3/library/functools.html>)  

### GitHub

* [Udacity's "Writing README Files" Course](<https://classroom.udacity.com/courses/ud777>)  

* [Udacity's Blog: Open Source Projects](<https://blog.udacity.com/2013/10/get-started-with-open-source-projects.html>)  

* [Udacity's `Git Commit Message Style Guide`](<https://udacity.github.io/git-styleguide/>)  

* [Git: set `nano` as default editor](<https://www.oreilly.com/library/view/gitlab-cookbook/9781783986842/apas07.html>)  