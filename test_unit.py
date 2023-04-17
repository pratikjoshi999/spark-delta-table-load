import unittest
from pyspark.sql import SparkSession
import os

class TestSparkJobs(unittest.TestCase):

    
    def setUpClass(cls):
        """
        Create a SparkSession and set the path for the Delta table.
        """
        cls.spark = SparkSession.builder.appName("unit-tests").master("local[*]").getOrCreate()
        cls.delta_table_path = "/FileStore/tables/delta_table"

    def test_create_delta_table(self):
        """
        Test that the Delta table is created with the expected schema.
        """

	# Define Schema for Delta Table
	columns=sklearn.datasets.fetch_california_housing().feature_names
	schema = [i+' DOUBLE' for i in columns]
	schema=','.join(schema)


        # Create the Delta table
        self.spark.sql("CREATE TABLE IF NOT EXISTS housing_dataset ({}) USING DELTA " \
          "OPTIONS (PATH '/FileStore/tables/delta_table') ".format(schema) )
        
        # Check that the table exists and has the expected schema
        table_exists = self.spark.catalog._jcatalog.tableExists("housing_dataset")
        self.assertTrue(table_exists)

        table = self.spark.table("housing_dataset")
        expected_columns = set(['MedInc','HouseAge','AveRooms','AveBedrms','Population','AveOccup','Latitude','Longitude'])
        actual_columns = set(table.columns)
        self.assertEqual(actual_columns, expected_columns)

    def test_create_delta_table_invalid_schema(self):
    	"""
    	Test that an error is raised if an invalid schema is provided for the Delta table.
    	"""
    	with self.assertRaises(Exception) as context:
       		# Create the Delta table with an invalid schema
        	self.spark.sql("CREATE TABLE IF NOT EXISTS housing_dataset USING DELTA " \

    def test_ingest_data_into_delta_table(self):
        """
        Test that data can be ingested into the Delta table and that the table has the expected number of rows.
        """
        # Ingest data into the Delta table
        data = pd.DataFrame(sklearn.datasets.fetch_california_housing().data,
                            columns=sklearn.datasets.fetch_california_housing().feature_names)
        spark_df = self.spark.createDataFrame(data)
        spark_df.write.format("delta").mode("append").save(self.delta_table_path)

        # Check that the table has the expected number of rows
        table = self.spark.table("housing_dataset")
        expected_rows = len(data)
        actual_rows = table.count()
        self.assertEqual(actual_rows, expected_rows)


                      

                      


if __name__ == '__main__':
    unittest.main()
