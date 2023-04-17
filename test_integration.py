import unittest
from pyspark.sql import SparkSession

class TestCreateDeltaTableJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a SparkSession object
        cls.spark = SparkSession.builder.appName("test_create_delta_table_job").getOrCreate()

        # Define the path to the Delta table
        cls.delta_table_path = "/path/to/housing_dataset_delta"

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkSession object
        cls.spark.stop()

    def test_create_delta_table_job(self):
        """
        Test that the job creates the Delta table with the expected schema.
        """
        # Run the job
        create_delta_table_job()

        # Check that the Delta table has the expected schema
        table = self.spark.table("housing_dataset")
        expected_schema = ['MedInc','HouseAge','AveRooms','AveBedrms','Population','AveOccup','Latitude','Longitude'])
        actual_schema = ", ".join([str(s) for s in table.schema])
        self.assertEqual(actual_schema, expected_schema)

if __name__ == '__main__':
    unittest.main()
