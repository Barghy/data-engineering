![RedshiftLogo](https://dbdb.io/media/logos/amazon-redshift.png)

# Data Warehouses with AWS Redshift and S3
</br>

1. [**Introduction**](#INTroduction)
    - [Objectives](#objectives)
    - [Requirements](#requirements)

2. [**Solution**](#solution)
    - [Data Source](#data-source)
    - [Data Model](#data-model)
    - [File Structure](#file-structure)
    - [Running the Solution](#running-the-solution)

3. [**Technologies**](#technologies)
    - [Tools](#tools)
    - [Packages](#packages)

4. [**Contact**](#contact)

</br>

## Introduction
### Objectives

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project demonstrates the following skills:
- Provisioning of cloud infrastructure from code and config files
- Building ETL pipelines that copy data from S3 into staging tables in Redshift
- Transformation of data INTo a set of dimensional tables for further analysis
- Data quality checks along the pipelines to data has been copied and inserted
- Deleting cloud infrastructure once project needs to be decomissioned
- Version control using git and GitHub including README documentation

</br>

### Requirements

| Requirement      | Checklist                                                                             |
|------------------|---------------------------------------------------------------------------------------|
| Table Creation   | :heavy_check_mark: Table creation script runs without errors                          |
| Table Creation   | :heavy_check_mark: Staging tables are properly defined                                |
| Table Creation   | :heavy_check_mark: Fact and dimensional tables for a star schema are properly defined |
| ETL              | :heavy_check_mark: ETL script runs without errors                                     |
| ETL              | :heavy_check_mark: ETL script properly processes transformations in Python            |
| Code Quality     | :heavy_check_mark: The project shows proper use of documentation                      |
| Code Quality     | :heavy_check_mark: The project code is clean and modular                              |
| Data Quality     | :heavy_check_mark: Data quality checks present in ETL                                 |

</br>

## Solution

### Data Source 

**song_data** 
</br>
- Each file is in json format and contains metadata about a song and the artist of that song.
- The files are partionioned by the first three letters of each song's track ID.
- Sample file path: song_data/A/B/C/TRABCEI128F424C983.json
```
s3://udacity-dend/song-data
``` 

**log_data** 
</br>
- Each file is in json format and contains app activity and configuration settings from the music streaming app.
- The files are partionioned by year and month.
- Sample file path: log_data/2018/11/2018-11-12-events.json
```
s3://udacity-dend/log-data
s3://udacity-dend/log_json_path.json
```

</br>

### Data Model

The staging, fact and dimension tables are defined as below.

| STAGING **staging_events** |             |
|----------------------------|-------------|
| artist                     | `VARCHAR`   |
| auth                       | `VARCHAR`   |
| firstName                  | `VARCHAR`   |
| gender                     | `VARCHAR`   |
| itemInSession              | `INTEGER`   |
| lastName                   | `VARCHAR`   |
| length                     | `FLOAT`     |
| level                      | `VARCHAR`   |
| location                   | `VARCHAR`   |
| method                     | `VARCHAR`   |
| page                       | `VARCHAR`   |
| registration               | `FLOAT`     |
| sessionId                  | `INTEGER`   |
| song                       | `VARCHAR`   |
| status                     | `INTEGER`   |
| ts                         | `TIMESTAMP` |
| userAgent                  | `VARCHAR`   |
| userId                     | `INTEGER`   |

</br>

| STAGING **staging_songs**  |             |
|----------------------------|-------------|
| num_songs                  | `INTEGER`   |
| artist_id                  | `VARCHAR`   |
| artist_latitude            | `FLOAT`     |
| artist_longitude           | `FLOAT`     |
| artist_location            | `VARCHAR`   |
| artist_name                | `VARCHAR`   |
| song_id                    | `VARCHAR`   |
| title                      | `VARCHAR`   |
| duration                   | `FLOAT`     |
| year                       | `INTEGER`   |

</br>

| FACT **songplays**         |             |
|----------------------------|-------------|
| songplay_id `PK` `IDENTITY`| `INT`       |
| start_time `FK` `NOT NULL` | `TIMESTAMP` |
| user_id `FK` `NOT NULL`    | `INT`       |
| level                      | `TEXT`      |
| song_id `FK` `NOT NULL`    | `TEXT`      |
| artist_id `FK` `NOT NULL`  | `TEXT`      |
| session_id                 | `INT`       |
| location                   | `TEXT`      |
| user_agent                 | `TEXT`      |

</br>

| DIM **users**              |             |
|----------------------------|-------------|
| user_id `PK` `NOT NULL`    | `INT`       |
| first_name `NOT NULL`      | `TEXT`      |
| last_name `NOT NULL`       | `TEXT`      |
| gender                     | `TEXT`      |
| level                      | `TEXT`      |

</br>

| DIM **song**               |             |
|----------------------------|-------------|
| song_id `PK` `NOT NULL`    | `TEXT`      |
| title `NOT NULL`           | `TEXT`      |
| artist_id `NOT NULL`       | `TEXT`      |
| year `NOT NULL`            | `INT`       |
| duration `NOT NULL`        | `FLOAT`     |

</br>

| DIM **artist**             |             |
|----------------------------|-------------|
| artist_id `PK` `NOT NULL`  | `TEXT`      |
| name `NOT NULL`            | `TEXT`      |
| location                   | `TEXT`      |
| latitude                   | `FLOAT`     |
| longitude                  | `FLOAT`     |

</br>

| DIM **time**               |             |
|----------------------------|-------------|
| start_time `PK` `NOT NULL` | `TIMESTAMP` |
| hour `NOT NULL`            | `INT`       |
| day `NOT NULL`             | `INT`       |
| week `NOT NULL`            | `INT`       |
| month `NOT NULL`           | `INT`       |
| year `NOT NULL`            | `INT`       |
| weekday `NOT NULL`         | `TEXT`      |

</br>

### File Structure

```
📦 03-data-warehouse-with-redshift
 ┣ README.md
 ┣ dwh.cfg
 ┣ sql_queries.py 
 ┣ build_infra.py
 ┣ create_tables.py
 ┣ etl.py
 ┗ delete_infra.py
```

</br>

### Running the Solution

**1. config**
</br>
You will need to input your own configurations (*****) in the `dwh.cfg` file below to run the solution. This file must be placed in the project root folder as displayed in the [file structure](#file-structure) section of this README.
</br>

```
[CLUSTER]
HOST=*****.us-west-2.redshift.amazonaws.com
DB_NAME=*****
DB_USER=*****
DB_PASSWORD=*****
DB_PORT=5439
REGION=us-west-2

[IAM_ROLE]
ARN=arn:aws:iam::*****

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=*****
SECRET=*****

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_CLUSTER_IDENTIFIER=*****
DWH_DB=*****
DWH_DB_USER=*****
DWH_DB_PASSWORD=*****
DWH_PORT=5439
DWH_IAM_ROLE_NAME=*****
```

**2. Install Dependencies**
</br>
Run the below in the command line to install all of the required dependencies for this project.
```
$ sudo apt-get install aws-cli
$ pip install boto3 botocore pandas psycopg2 pycopg2-binary
```
**NOTE:** if you are using macos please use `$ brew install` in place of `$ sudo apt-get install`

**3. Build Infrastructure**
</br>
Run the script below to provision all of the necessary cloud infrastructure from the config file for this project. Each stage of the process can be easily followed in the terminal.
```
$ python build_infra.py
```

**4. Create Tables**
</br>
The script below will drop any existing and create new tables based on the queries defined in the sql_queries.py file under drop_table_queries and create_table_queries. Each stage of the process can be easily followed in the terminal.
```
$ python create_table.py
```

**5. Run ETL**
</br>
Finally, we can run the ETL pipeline which will copy the data from S3, load INTo the staging tables, and insert INTo the final table for analytics. These queries are based on the copy_table_queries and insert_table_queries in sql_queries.py. 
</br>
Each stage of the process can be easily followed in the terminal, including validation of the row count to check that data has been loaded ito each table as expected.
```
$ python etl.py
```

**OPTIONAL: Delete Infrastructure**
</br>
If you do not need to keep the data warehouse up and running for analysis, this script will delete all of the infrastructure so that you are not charged for unused capacity. This is good practice to reduce costs and avoid large bills from AWS.
```
$ python delete_infra.py
```

</br>

## Technologies

### Tools

`aws s3`

`aws redshift`

`aws cli`

`python` :snake:

`sql`

`vscode`

`git`

`terminal (linux)` :penguin:

</br>

### Packages

`pandas` :panda_face:

`psycopg2`

`botocore`

`boto3`

`re`

`configparser`

`json`

`time`

</br>

## Contact

This project was created with :heart: by @Barghy for the Udacity Data Engineering Nanodegree Program

For futher info please contact <barghy@gmail.com>