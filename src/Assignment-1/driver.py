from pyspark.sql import SparkSession
from utils import find_unique_locations, find_products_by_user, total_spending_by_user_product

def main():
    spark = SparkSession.builder.appName("SparkAssignment").getOrCreate()

    # Here i Upload path of csv file
    users_df = spark.read.csv("E:/Spark_Assignment-1/Resource/user.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv("E:/Spark_Assignment-1/Resource/transaction.csv", header=True, inferSchema=True)

    # Now we have to  Count of unique locations where each product is sold

    unique_locations_count = find_unique_locations(transactions_df)
    unique_locations_count.show()

    # Here i Find out products bought by each user
    products_by_user = find_products_by_user(transactions_df)
    products_by_user.show()

    # c) Now Total spending done by each user on each product
    total_spending = total_spending_by_user_product(transactions_df)
    total_spending.show()

    spark.stop()



if __name__ == "__main__":
    main()
