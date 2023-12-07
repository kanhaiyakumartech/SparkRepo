from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SimpleRead").getOrCreate()

# Define the file path
file_path = "E:/Spark_Assignment-1/transaction.csv"
file_path2 = "E:/Spark_Assignment-1/user.csv"

# Read the CSV file into a DataFrame
def read_csv( file_path, header=True):
    return spark.read.csv(file_path, header=header, inferSchema=True)

df=read_csv(file_path)

read=read_csv(file_path2)
# Show the first few rows of the DataFrame
df.show()
print("This is first file :-")
read.show()
print("this is 2nd file")



