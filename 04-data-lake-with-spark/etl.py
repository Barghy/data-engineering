import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: initiates the spark session for data processing 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: processes song data from inital files into final tables and saves to s3 bucket

        Parameters:
            spark:          the spark session
            input_data:     path to the s3 bucket containing initial log data
            output_data:    path to the s3 bucket containing final tables
    """
    # get filepath to song data file
    print("Loading Song Data")
    song_data = input_data + 'song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)
    print("Song Data Loaded")
    
    # extract columns to create songs table
    print("STARTING: Song Table")
    songs_table = df.select("song_id","title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    print("Saving Song Table")
    (
        songs_table
        .write
        .partitionBy("year", "artist_id")
        .format("parquet")
        .save(output_data + 'songs/', mode='overwrite')
    )
    print("COMPLETED: Song Table")
    
    # extract columns to create artists table
    print("STARTING: Artist Table")
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")

    # write artists table to parquet files
    print("Saving Artist Table")
    (
        artists_table
        .write
        .format("parquet")
        .save(output_data + 'artists/', mode='overwrite')
    )
    print("COMPLETED: Artist Table")


def process_log_data(spark, input_data, output_data):
    """
        Description: processes log data from inital files into final tables and saves to s3 bucket

        Parameters:
            spark:          the spark session
            input_data:     path to the s3 bucket containing initial log data
            output_data:    path to the s3 bucket containing final tables
    """
    # get filepath to log data file
    print("Loading Log Data")
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.load(log_data, format='json')
    print("Log Data Loaded")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table 
    print("STARTING: Users Table")   
    users_table = df.select("userId","firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    print("Saving Users Table")
    (
        users_table
        .write
        .format('parquet')
        .save(output_data + 'users/', mode='overwrite')
    )
    print("COMPLETED: Users Table") 

    # create timestamp column from original timestamp column
    print("STARTING: Time Table")  
    def get_timestamp(ts):
        """
            Description: converts the column to a timestamp

            Parameters:
                ts:     the timestamp column to convert
        """
        return to_timestamp(col(ts) / 1000)
    spark.udf.register("getTimestamp", get_timestamp, TimestampType())
    
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = (
        df
        .select("start_time")
        .dropDuplicates()
        .withColumn("hour", hour(col('start_time')))
        .withColumn("day", dayofmonth(col('start_time')))
        .withColumn("week", weekofyear(col('start_time')))
        .withColumn("month", month(col('start_time')))
        .withColumn("year", year(col('start_time')))
        .withColumn("weekday", date_format(col('start_time'), 'E'))
    )
    
    # write time table to parquet files partitioned by year and month
    print("Saving Time Table")
    (
        time_table
        .write
        .partitionBy("year", "month")
        .format("parquet")
        .save(output_data + 'time/', mode='overwrite')
    )
    print("COMPLETED: Time Table") 

    # read in song data to use for songplays table
    print("STARTING: Songplays Table")  
    song_df = spark.read.load(output_data + 'songs/*/*/*', format='parquet')
    artist_df = spark.read.load(output_data + 'artists/*', format='parquet')

    song_log = (
    df
    .join(
        song_df, 
        df.song == song_df.title)
    )

    songplay_join = (
        song_log
        .join(
            artist_df, 
            song_log.artist == artist_df.artist_name)
    )

    # extract columns from joined song and log datasets to create songplays table 
    songplays = (
        songplay_join
        .join(
                time_table, 
                songplay_join.start_time == time_table.start_time, 
                'left')
        .drop(songplay_join.start_time)
    )

    songplays_table = (
        songplays
        .select(
            col('start_time'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('song_id').alias('song_id'),
            col('artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'),
            col('userAgent').alias('user_agent'),
            col('year').alias('year'),
            col('month').alias('month'),
        ).repartition("year", "month")
    )

    # write songplays table to parquet files partitioned by year and month
    print("Saving Songplays Table")
    (
        songplays_table
        .write
        .partitionBy("year", "month")
        .format("parquet")
        .save(output_data + 'songplays/', mode='overwrite')
    )
    print("COMPLETED: Songplays Table") 


def main():
    """
        Description: extract json files from S3 bucket, transform into final dimension tables and load back to S3 as parquet
    """
    print("1/7: Creating Spark Session")
    spark = create_spark_session()
    print("2/7: Spark Session Created")
    
    input_data = config['S3']['INPUT_DATA']
    output_data = config['S3']['OUTPUT_DATA']
    print("3/7: Configurations Loaded")
    
    print("4/7: Processing Song Data")
    process_song_data(spark, input_data, output_data)    
    print("5/7: Song Data Processed")
    
    print("6/7: Processing Log Data")
    process_log_data(spark, input_data, output_data)
    print("7/7: Log Data Processed")
    
    spark.stop()

if __name__ == "__main__":
    main()