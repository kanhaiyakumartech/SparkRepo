from pyspark.sql import *
from pyspark.sql.functions import *

def create_spark_session():
    spark = SparkSession.builder.appName("Question_2").getOrCreate()
    return spark

def create_rdd(spark, filepath):
    rdd = spark.sparkContext.textFile(filepath)
    return rdd

def count_lines(rdd):
    count_of_lines = rdd.count()
    return count_of_lines

def read_file(spark, filepath):
    df = spark.read.text(filepath)
    return df

def process_dataframe(df):
    df1 = df.withColumn("log message", split(col("value"), ",").getItem(0))\
        .withColumn("timestamp", split(col("value"), ",").getItem(1))\
        .withColumn("downloader_id", split(col("value"), ",").getItem(2)).drop(col("value"))
    df2 = df1.withColumn("downloader", split(col("downloader_id"), "--").getItem(0))\
        .withColumn("repository_client", split(col("downloader_id"), "--").getItem(1)).drop("downloader_id")
    df_torr = df2.withColumn("repository_clients", split(col("repository_client"), ":").getItem(0))\
        .withColumn("commit_message", split(col("repository_client"), ":").getItem(1)).drop("repository_client")
    return df_torr

def count_warning_messages(df_torrent):
    warn_msg = df_torrent.filter(col("log message") == "WARN")
    warn_count = warn_msg.count()
    return warn_count

def count_api_clients(df_torrent):
    processed_repositories = df_torrent.filter(col("repository_clients").like('%api_client%'))
    processed_repos = processed_repositories.count()
    return processed_repos

def find_most_http_requests(df_torrent):
    http = df_torrent.filter(col("commit_message").like('%https%'))
    most_requests_http = http.groupBy("repository_clients").count().orderBy("count", ascending=False).limit(1).drop("count")
    return most_requests_http

def find_most_failed_http_requests(df_torrent):
    failed_http = df_torrent.filter(col("commit_message").like('%Failed%'))
    more_failed_http = failed_http.groupBy("repository_clients").count().orderBy("count", ascending=False).limit(1).drop("count")
    return more_failed_http

def find_most_active_hours(df_torrent):
    active_hour = df_torrent.withColumn("hour", hour("timestamp"))
    active_hours = active_hour.groupBy("hour").count().orderBy("count", ascending=False).limit(1).drop("count")
    return active_hours

def find_most_active_repository(df_torrent):
    active_repo = df_torrent.filter(col("repository_clients") == " ghtorrent.rb")
    return active_repo
    
