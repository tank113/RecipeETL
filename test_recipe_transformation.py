import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from main import extract_recipes, add_difficulty_level, aggregate_total_minutes

class TestRecipeFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('Test').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_recipes(self):
        schema = StructType([
            StructField("ingredients", StringType(), True),
        ])
        data = [("chilies and tomatoes",),
                ("onion and garlic",),
                ("chillies, peppers, and onions",),
                ("potatoes and carrots",)]
        df = self.spark.createDataFrame(data, schema)

        result_df = extract_recipes(df)

        # Assertions
        self.assertEqual(result_df.count(), 2) 

    def test_add_difficulty_level(self):
        schema = StructType([
            StructField("cookTime", StringType(), True),
            StructField("prepTime", StringType(), True),
        ])
        data = [(None, "PT10M"),
                ("PT30M", "PT25M"),
                ("PT45M", "PT20M"),
                ("PT70M", "PT40M")]
        df = self.spark.createDataFrame(data, schema)

        result_df = add_difficulty_level(df)

        self.assertIn("Difficulty", result_df.columns) 

    def test_aggregate_total_minutes(self):
        schema = StructType([
            StructField("Difficulty", StringType(), True),
            StructField("totalTimeMinutes", StringType(), True),
        ])
        data = [("Easy", "50"),
                ("Medium", "40"),
                ("Hard", "70"),
                ("Medium", "55")]
        df = self.spark.createDataFrame(data, schema)

        result_df = aggregate_total_minutes(df)

        self.assertEqual(result_df.count(), 3)

if __name__ == '__main__':
    unittest.main()
