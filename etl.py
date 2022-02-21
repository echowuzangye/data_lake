import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    instantiate a spark session
    """
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
            .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
            .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    read song_data file from s3
    create songs_table and write it to parquet files partitioned by year and artist
    create artists_table and write it to parquet files
    """
    
    song_data = input_data+"song_data/A/A/A/*.json"

    df = spark.read.json(song_data)

    songs_table = df.select('song_id',
                          'title',
                          'artist_id',
                          'year',
                          'duration').dropDuplicates(['song_id'])
    
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet", 'overwrite')

    artists_table = df.select('artist_id',
                          col('artist_name').alias('name'),
                          col('artist_location').alias('location'),
                          col('artist_latitude').alias('latitude'),
                          col('artist_longitude').alias('longitude')).dropDuplicates(['artist_id'])
    
    artists_table.write.parquet(output_data + "artists.parquet",'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    read log_data file from s3
    create users_table and write it to parquet files
    create datetime column and use this column to create time_table,  
    write it to parquet files partitioned by year and month
    read song_data file again and create songplays_table from the joined dataset,
    write it to parquet files partitioned by year and month
    """
    log_data = input_data+"log_data/*/*/*.json"

    df = spark.read.json(log_data)

    df = df.where(df.page == "NextSong")
  
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level').dropDuplicates(['user_id'])
    
    users_table.write.parquet(output_data + "users.parquet",'overwrite')

    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(int(x) / 1000)), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: date_format(x,"yyyy-MM-dd"))
    df = df.withColumn('date', get_datetime(col('start_time')))
    
    time_table = df.select(col('start_time'),
                       hour('date').alias('hour'),
                       dayofmonth('date').alias('day'),
                       weekofyear('date').alias('week'),
                       month('date').alias('month'),
                       year('date').alias('year'),
                       date_format('date','E').alias('weekday')
                      ).distinct()
    
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet",'overwrite')

    song_df = spark.read.json(input_data+"song_data/A/A/A/*.json")
    df.createOrReplaceTempView("log_data")
    song_df.createOrReplaceTempView("song_data")
 
    songplays_table = spark.sql("""
                            SELECT monotonically_increasing_id() as songplay_id,
                            start_time,
                            year(start_time) as year,
                            month(start_time) as month,
                            userId as user_id,
                            level,
                            sd.song_id,
                            sd.artist_id,
                            sessionId as session_id,
                            location,
                            userAgent as user_agent FROM log_data as ld
                            JOIN song_data as sd
                            ON ld.song = sd.title
                            AND ld.artist = sd.artist_name
                            AND ld.length = sd.duration
                            """)

    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays.parquet",'overwrite')


def main():
    """
    create a spark session
    read song and log data
    use the datasets to create five tables
    write tables back to s3 bucket as parquet files
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://echocao-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
