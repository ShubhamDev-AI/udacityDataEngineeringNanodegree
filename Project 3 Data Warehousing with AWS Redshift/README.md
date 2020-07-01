
# Data Warehousing with AWS Redshift  
  
![AWS Redshift logo](./AWSRedshiftlogo.png)

This folder contains scripts relating to creation and ETL processes of the `sparkify_dw` database and all of its tables.  
  
 The purpose of this Data Warehouse is to simplify and enhance performance of analytical queries from Sparkify Music Streaming app's Data Analysts team.  

 ## Database Architecture

 The database has two schemas (Entity Relationship Diagram available below):
* `staging`: raw data ingestion happens here.
* `public`: modeled as a dimensional Star Schema, curated data sits here after an ELT process takes place.  
  
![ER Diagram](./stagingAndStarSchemaERDiagram.png)

**Data distribution accross cluster nodes:**  

All Dimension tables have their `DISTSTYLE` property set to `ALL`: this ensures they're available in all cluster nodes, avoiding shuffling overhead when joining Fact and Dimension tables.  
Fact tables have their `DISTSTYLE` property set to `EVEN`: data is then distributed in a round-robin fashion accross cluster nodes.  
`SORTKEYS` are set for each table to enhance data access performance. Details can be seen at their `CREATE TABLE` statements.

**Overview of each schema's tables:**

**staging.songs:**
> **Type:** Staging  
**Grain:** one row per artist and song title  
**Description:** raw data ingested from "song tracks" JSON files.

**staging.events:**  
> **Type:** Staging  
**Grain:** one row per user and website interaction event  
**Description:** raw data ingested from daily event logs JSON files.

**public.dim_artists:**  
> **Type:** Dimension  
**Grain:** one row per artist  
**Description:** contains artist's attributes like names, location, etc.  

**public.dim_users:**  
> **Type:** Dimension  
**Grain:** one row per user and subscription level change date  
**Description:**  contains users' attributes like names, gender and so forth.  
***Notes:*** user subscription level changes are treated as a Type 2 Slowly Changing Dimension (each change to subscription level triggers the INSERT of a new row for the user along with timestamps of "validity" for its subscription option).

**public.dim_songs:**  
> **Type:** Dimension  
**Grain:** one row per artist and song title.  
**Description:** contains data on Artist's songs like song titles, year of release, duration, etc.

**public.dim_time:**  
> **Type:** Dimension  
**Grain:** one row per event timestamp  
**Description:** A calendar table with descriptive attributes for dates and time.

**public.fact_songplays:**  
> **Type:** Fact  
**Grain:** one row per song execution event.  
**Description:** contains data on users' song execution events and the foreign keys to their corresponding dimensions.  
***Notes:*** *referential integrity is INFORMED by Redshift's* `REFERENCES` Foreign Key Constraint (Redshift's architecture doesn't enforce referential integrity: it's the ETL process' responsibility to enforce this).

## Data Quality:  

When processing the raw data files, all attributes who are not "ID's" AND have missing values are replaced with the default values below, according to their data types:  

**Decimal Numbers:** replaced by the `9,999,999.99` value  
**Text:** replaced by the `'Unknown'` string  
**Small Integers:** replaced by the `9,999` value  
**Integers:** replaced by the `9,999,999` value  
  
These default values are applied by using SQL's `COALESCE()` function in each `INSERT` statement moving data from Staging tables to Dimension/Fact tables.
   
This makes data quality issues easier to detect and report on, while also avoiding `NULL` entries in Dimensions tables.  
   
> Letting `NULL` entries in Dimension tables is a poor practice, as mentioned in Ralph Kimball's `The Data Warehouse Toolkit` book, page 509. Conversely, `NULL` values are OK in Fact table **metrics** (not in their foreing keys!), as SQL and other analysis tools know how to handle `NULLS` properly when performing calculations upon them.  
  
## Running the Scripts:  
  
The scripts in this repository are explained below, and must be run in the following sequence:

### 1 - `configFileSetup.py`  

Run this script to create a configuration file that'll be used throughout this project.  
The following items must be specified in `configFileSetup.py`'s template. The resulting config file will look like this:  
> **[aws_credentials]**  
access_key_id = *your_aws_acces_key_id*  
secret_access_key = *your_aws_secret_key_id*  
**[cluster_settings]**  
redshift_role_name = *aws_role_name_to_create*  
redshift_role_arn = *written_back_by_`infrastructureSetup.ipynb`*  
cluster_type = *multi-node*  
number_of_nodes = *4*  
node_type = *dc2.large*  
cluster_identifier = *name_you'll_give_to_your_cluster*  
db_name = *database_name*  
master_user_name = *database's_first_super_user_name*  
master_user_password = *database's_first_super_user_password*  
port = *5439*  
endpoint = *written_back_by_`infrastructureSetup.ipynb`*  
**[s3]**  
log_data = *s3://udacity-dend/log_data*  
log_jsonpath = *s3://udacity-dend/log_json_path.json*  
song_data = *s3://udacity-dend/song_data*  

**IMPORTANT:** both "redshift_role_arn" and "endpoint" items will be programatically written to the config file when these objects are created by the next script: `infrastructureSetup.ipynb`.  

### 2 - `infrastructureSetup.ipynb`  
Infrastructure-as-Code ("Iac"), this notebook contains the steps (and their descriptions/related documentation) to programatically create and delete an AWS Redshift cluster. It's two main sections are:  

* **Cluster Setup Procedures:** reads config file, sets up Redshift cluster and tests it's connectivity;  
* **Cluster Take Down Procedures:** turns off Redshift cluster and detaches IAM roles previously associated with it;  
  
Notice this Notebook writes data back to the `dwh.cfg` config file previously created.

### 3 - `create_tables.py`  
Run this script to create all database schemas and tables needed for this project.  

### 4 - `etl.py`  
Run this script to perform a paralell load of JSON files located in multiple S3 Buckets. Data is first loaded into staging tables and then transformed to feed a set of Dimensional tables.  

### 5 - `sparkifyDataExploration.ipynb`  
This Jupyter Notebook contains examples of analytical queries performed on the resulting Dimensional Model and explains various concepts utilized during the development of this Data Warehouse.  







