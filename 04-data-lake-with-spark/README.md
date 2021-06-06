![SparkLogo](https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png)

# Data Lake with Spark
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

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project demonstrates the following skills:
- Provisioning of cloud infrastructure from code and config files
- Building ETL pipelines that extracts data from S3
- Transformation of data into a set of dimensional tables using Spark and loading back to S3 for further analysis
- Deleting cloud infrastructure once project needs to be decomissioned
- Version control using git and GitHub including README documentation

</br>

### Requirements

| Requirement      | Checklist                                                            |
|------------------|----------------------------------------------------------------------|
| ETL              | :heavy_check_mark: etl script runs without errors                    |
| ETL              | :heavy_check_mark: Analytics tables are correctly organized on S3    |
| ETL              | :heavy_check_mark: The correct data is included in all tables        |
| Code Quality     | :heavy_check_mark: The project shows proper use of documentation     |
| Code Quality     | :heavy_check_mark: The project code is clean and modular             |

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
```

</br>

### File Structure

```
📦 04-data-lake-with-spark
 ┣ README.md
 ┣ dl.cfg 
 ┗ etl.py
```

</br>

### Running the Solution

**1. config**
</br>
You will need to input your own configurations `*****` in the `dl.cfg` file below to run the solution. This file must be placed in the project root folder as displayed in the [file structure](#file-structure) section of this README.
</br>

```
[AWS]
AWS_ACCESS_KEY_ID=*****
AWS_SECRET_ACCESS_KEY=*****

[S3]
INPUT_DATA=s3a://udacity-dend/
OUTPUT_DATA=s3a://*****/
```

**2. Install Dependencies**
</br>
Run the below in the command line to install all of the required dependencies for this project.
```
$ pip install pyspark
```

**3. Run ETL**
</br>
Run etl.py to load the data from S3, transform into the dimensional tables required and load to an output folder defined in the config file. 
</br>
```
$ python etl.py
```

</br>

## Technologies

### Tools

`aws s3`

`spark`

`python` :snake:

`vscode`

`git`

`terminal (linux)` :penguin:

</br>

### Packages

`pyspark` :panda_face:

`configparser`

`datetime`

`os`

</br>

## Contact

This project was created with :heart: by @Barghy for the Udacity Data Engineering Nanodegree Program

For futher info please contact <barghy@gmail.com>