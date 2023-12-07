from utils import *

spark = create_spark_session()
filepath = "E:/Spark_Assignment-1/ghtorrent-logs.txt"

# 1. Loading data into RDD
rdd = create_rdd(spark, filepath)

# 2. Counting number of lines in RDD
num_lines = count_lines(rdd)
print("Number of lines in RDD:", num_lines)

# 3. Counting number of WARNING messages
df = read_file(spark, filepath)
df_torrent = process_dataframe(df)
num_warnings = count_warning_messages(df_torrent)
print("Number of WARNING messages:", num_warnings)

# 4. Counting total number of processed repositories (api_client lines only)
num_api_clients = count_api_clients(df_torrent)
print("Total number of processed repositories (api_client):", num_api_clients)

# 5. Finding the client with the most HTTP requests
most_http_client = find_most_http_requests(df_torrent)
most_http_client.show()

# 6. Finding the client with the most FAILED HTTP requests
most_failed_http_client = find_most_failed_http_requests(df_torrent)
most_failed_http_client.show()

# 7. Determining the most active hour of the day
most_active_hour = find_most_active_hours(df_torrent)
most_active_hour.show()

# 8. Identifying the most active repository (using messages from ghtorrent.rb layer only)
most_active_repository = find_most_active_repository(df_torrent)
most_active_repository.show()