import unittest
from unittest.mock import MagicMock
from SparkRepo.src.Assignment-2.utils import *

class TestUtilFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()
        cls.filepath =r"E:/Spark_Assignment-1/Assignment2_test_data.txt"

    def test_create_spark_session(self):
        result = create_spark_session()
        self.assertIsInstance(result, SparkSession)

    def test_create_rdd(self):
        test_rdd = self.spark.sparkContext.parallelize(["line1", "line2", "line3"])
        test_rdd.count = MagicMock(return_value=3)
        result = count_lines(test_rdd)
        self.assertEqual(result, 3)

    def test_read_file(self):
        df = self.spark.read.text(self.filepath)
        result = read_file(self.spark, self.filepath)
        self.assertIsNotNone(result)

    def test_process_dataframe(self):
        mock_df = self.spark.createDataFrame([("value1", "value2", "value3")], ["value"])
        result = process_dataframe(mock_df)
        self.assertIsNotNone(result)

    def test_count_warning_messages(self):
        mock_df = self.spark.createDataFrame([("WARN",)], ["log message"])
        result = count_warning_messages(mock_df)
        self.assertEqual(result, 1)

    def test_count_api_clients(self):
        mock_df = self.spark.createDataFrame([("api_client",)], ["repository_clients"])
        result = count_api_clients(mock_df)
        self.assertEqual(result, 1)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()

