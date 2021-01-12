![PostgreSQLLogo](https://niquola.github.io/hl7-russia-2014-fhir-slides/pg.png)

# Relational Data Modelling with PostgreSQL

1. [**Introduction**](#introduction)
    - [Objectives](#objectives)
    - [Requirements](#requirements)

2. [**Solution**](#solution)
    - [Data Model](#data-model)
    - [File Structure](#file-structure)
    - [Running the Solution](#running-the-solution)

3. [**Technologies**](#technologies)
    - [Tools](#tools)
    - [Packages](#packages)

4. [**Contact**](#contact)

## Introduction

### Objectives

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project demonstrates the following skills:
- Create a Postgres database with tables designed to optimise queries from the data
- Define fact and dimension tables for a star schema 
- Build an ETL pipeline using Python that transfers data from multiple files in two local directories 
- Insert these tables in Postgres using Python and SQL
- Test the database and ETL pipeline by validating against queries shared by the Sparkify analytics team
- Clearly document the project and push the solution to GitHub

## Requirements

| Requirement      | Checklist                                                                             |
|------------------|---------------------------------------------------------------------------------------|
| Table Creation   | :heavy_check_mark: Table creation script run without errors                           |
| Table Creation   | :heavy_check_mark: Fact and dimensional tables for a star schema are properly defined |
| ETL              | :heavy_check_mark: script runs without errors                                         |
| ETL              | :heavy_check_mark: ETL script properly processes transformations in Python            |
| Code Quality     | :heavy_check_mark: The project shows proper use of documentation                      |
| Code Quality     | :heavy_check_mark: The project code is clean and modular                              |
| Backlog          | :x: Bulk insert log files using COPY rather than INSERT                               |
| Backlog          | :x: Add data quality checks                                                           |
| Backlog          | :x: Create a dashboard for analytic queries on the database                           |

## Solution

### Data Model

The fact and dimension tables are defined as below:

| FACT **songplays**     |             |
|------------------------|-------------|
| songplay_id `PK`       | `serial`    |
| start_time `FK`        | `timestamp` |
| user_id `FK`           | `int`       |
| level                  | `text`      |
| song_id `FK`           | `text`      |
| artist_id `FK`         | `text`      |
| session_id             | `int`       |
| location               | `text`      |
| user_agent             | `text`      |

| DIM **users**          |             |
|------------------------|-------------|
| user_id `PK`           | `int`       |
| first_name             | `text`      |
| last_name              | `text`      |
| gender                 | `text`      |
| level                  | `text`      |


| DIM **songs**          |             |
|------------------------|-------------|
| song_id `PK`           | `text`      |
| title                  | `text`      |
| artist_id `FK`         | `text`      |
| year                   | `int`       |
| duration               | `float`     |

| DIM **artists**        |             |
|------------------------|-------------|
| artist_id `PK`         | `text`      |
| name                   | `text`      |
| location               | `text`      |
| latitude               | `float`     |
| longitude              | `float`     |

| DIM **time**           |             |
|------------------------|-------------|
| start_time `PK`        | `timestamp` |
| hour                   | `int`       |
| day                    | `int`       |
| week                   | `int`       |
| month                  | `int`       |
| year                   | `int`       |
| weekday                | `text`      |

### File Structure

```
📦 01-data-modelling-with-postgres
 ┣ README.md
 ┣ 📂data
 ┃ ┣ 📂log_data
 ┃ ┗ 📂song_data
 ┣ sql_queries.py
 ┣ create_tables.py
 ┣ etl.ipynb
 ┣ etl.py
 ┗ test.ipynb
```

**data**

`log_data` each file is in json format and contains activity logs from the music streaming app. The files are partionioned by uear and month.

`song_data` each file is in json format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

**tables**

`sql_queries` contains the sql which defines the DROP, CREATE and INSERT queries for every table. This is used in create_tables.py, etl.ipynb and etl.py.

`create_tables` drops and creates the tables in the database. This must always be run before etl.py.

**etl**

`etl.ipynb` reads and processes a single file from song_data and log_data to document detailed information about the ETL process.

`etl.py` reads and processes files from song_data and log_data and loads them into your tables.

**test**

`test.ipynb` displays the first 5 rows of each table to check the database has been created correctly.

### Running the Solution

1. Download all of the files from the repo, and update create_table.py and etl.py with you database credentials
2. Run create_tables.py to create your database and tables
3. Run etl.py to process the entire dataset and insert into the database
4. Run test.ipynb to confirm the creation of your tables with the correct columns

## Technologies

### Tools

`sql (postgresql)`

`python` :snake:

`vscode`

`terminal (linux)` :penguin:

### Packages

`pandas` :panda_face:

`psycopg2`

`os`

`glob`

## Contact

This project was created with :heart: by @Barghy for the Udacity Data Engineering Nanodegree Program

For futher info please contact <barghy@gmail.com>