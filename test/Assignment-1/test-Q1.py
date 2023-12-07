import unittest
from pyspark.sql import SparkSession
from src.Assignment-1.utils import find_unique_locations, find_products_by_user, total_spending_by_user_product

class TestUtils(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("UnitTest").getOrCreate()
        self.transactions_df = self.spark.createDataFrame([(1, "Kanhaiya", 101, 10.0, "Desc1"),
                                                           (2, "satyam", 102, 20.0, "Desc2"),
                                                           (3, "rajan", 101, 15.0, "Desc1")],
                                                          ["transaction_id", "product_id", "userid", "price", "product_description"])

    def test_find_unique_locations(self):
        result_df = find_unique_locations(self.transactions_df)
        # Add your assertions for the result_df

    def test_find_products_by_user(self):
        result_df = find_products_by_user(self.transactions_df)
        # Add your assertions for the result_df

    def test_total_spending_by_user_product(self):
        result_df = total_spending_by_user_product(self.transactions_df)
        # Add your assertions for the result_df

    def tearDown(self):
        self.spark.stop()

if __name__ == "__main__":
    unittest.main()
