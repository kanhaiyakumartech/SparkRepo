from pyspark.sql import functions as F

def find_unique_locations(userid):
    # Count of unique locations where each product is sold
    return userid.groupBy("product_id").agg(F.countDistinct("userid").alias("unique_locations"))

def find_products_by_user(transactions_df):
    # Find out products bought by each user
    return transactions_df.groupBy("userid").agg(F.collect_set("product_id").alias("products_bought"))

def total_spending_by_user_product(transactions_df):
    # Total spending done by each user on each product
    return transactions_df.groupBy("userid", "product_id").agg(F.sum("price").alias("total_spending"))

