![CassandraLogo](https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Cassandra_logo.svg/558px-Cassandra_logo.svg.png)

# Non-Relational Data Modelling with Apache Cassandra

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

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

This project demonstrates the following skills:
- Optimize the data model by creating tables in Apache Cassandra to run specfic queries
- Create an ETL pipeline that transfers data from .csv files to a streamlined .csv file
- Model and insert the data from the optimized .csv file to the tables in Apache Cassandra using Python 
- Test the data model and ETL pipeline by validating against queries shared by the Sparkify analytics team
- Clearly document the project and push the solution to GitHub

## Requirements

| Requirement             | Checklist                                                                                                             |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------|
| ETL Pipeline Processing | :heavy_check_mark: Student completes the ETL pipeline procedures                                                      |
| ETL Pipeline Processing | :heavy_check_mark: Student uses the correct datatype for each Cassandra `CREATE` statement                            |
| Data Modelling          | :heavy_check_mark: Student creates correct data models for the queries they need to run                               |
| Data Modelling          | :heavy_check_mark: Student can set up the data model correctly to generate the exact responses posed in the questions |
| Data Modelling          | :heavy_check_mark: Student models the data by using appropriate table names                                           |
| Data Modelling          | :heavy_check_mark: Student has given careful thought to how the data is modeled in the table and the sequence and order in which data is partitioned, inserted and retrieved from the table                                                                         |
| Primary keys            | :heavy_check_mark: The PRIMARY key for each table should uniquely identify each row in each of the tables             |
| Presentation            | :heavy_check_mark: Student provides responses to the questions                                                        |
| Presentation            | :heavy_check_mark: Students notebook code should be clean and modular                                                 |
| Backlog                 | :x: You can add description of your PRIMARY KEY and how you arrived at the decision to use each for the query         |
| Backlog                 | :x: Use Panda dataframes to add columns to your query output                                                          |

## Solution

### Data Model

Each table is designed for a particular query to provide low latency and high performance.

`K` denotes the partition key
`C` denotes the clustering column (inc sorting order)

| QUERY 1 **session_playlist** |         |
|------------------------------|---------|
| sessionId `K`                | `int`   |
| itemInSession `C-ASC`        | `int`   |
| artist                       | `text`  |
| song                         | `text`  |
| length                       | `float` |

| QUERY 2 **user_session**     |         |
|------------------------------|---------|
| userId `K`                   | `int`   |
| sessionId `C-ASC`            | `int`   |
| artist                       | `text`  |
| song                         | `text`  |
| firstName                    | `text`  |
| lastName                     | `text`  |
| itemInSession `C-ASC`        | `int`   |

| QUERY 3 **song_listeners**   |         |
|------------------------------|---------|
| song `K`                     | `text`  |
| firstName                    | `text`  |
| lastName                     | `text`  |
| userId `C-ASC`               | `int`   |

### File Structure

```
📦 02-data-modelling-with-cassandra
 ┣ README.md
 ┣ 📂event_data
 ┣ 📂images
 ┣ event_datafile_new.csv
 ┣ cassandra-pipeline.ipynb

```
**data**

`event_data` each file is in csv format and contains user details and listening history from the music streaming app. The files are partionioned by date.

`event_datafile_new` a file in csv format which is the output of the pre-processing of files in the event_data directory.

**notebook**

`cassandra-pipeline` contains the code and documentation to pre-process the data, run the etl and query the results of the pipeline

### Running the Solution

1. Download all of the files from the repo
2. Run the jupyter notebook

## Technologies

### Tools

`nosql (apache-cassandra)`

`python` :snake:

`vscode`

`terminal (linux)` :penguin:

### Packages

`pandas` :panda_face:

`cassandra` 

`os`

`glob`

`csv` 

## Contact

This project was created with :heart: by @Barghy for the Udacity Data Engineering Nanodegree Program

For futher info please contact <barghy@gmail.com>