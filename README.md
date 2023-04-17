# Spark Delta Table Project
This project creates a Delta table in Spark and ingests data into it from a source dataset. It includes two Spark jobs:

- A job that creates a Delta table with a specified schema.
- A job that ingests data into the Delta table from a source dataset.

The project also includes unit tests and integration tests to verify the functionality of the Spark jobs.

## Getting Started
To use this project, you'll need access to a Spark cluster with the appropriate dependencies installed. You'll also need access to the source data that will be ingested into the Delta table.

Prerequisites
Before running the Spark jobs, you'll need to install the necessary dependencies:


You can install these dependencies using pip:

```sh
pip install requirement.txt
```
## Running the Spark Jobs
To run the Spark jobs, you can use a notebook in a Spark cluster, or you can submit the jobs as standalone applications.

Running the jobs using a notebook
To run the Spark jobs using a notebook, follow these steps:

1. Open the notebook in your Spark cluster.
2. Copy and paste the code for the Spark jobs into the notebook.
3. Modify the parameters as necessary (e.g., the paths to the source data and the Delta table).
4. Run the notebook to execute the Spark jobs.

Running the jobs as standalone applications
To run the Spark jobs as standalone applications, first copy notebook code into 'job.py' then you can use the spark-submit command:

```
spark-submit job.py
```
## Running the Unit Tests
To run the unit tests, you'll need to install the pytest package:

```
pip install pytest
```
Then, you can run the tests using the pytest command:

```
pytest test_unit.py
```
## Running the Integration Tests
To run the integration tests, you'll need to have a Spark cluster with the appropriate dependencies installed, as well as access to the source data and the Delta table.

You can run the integration tests using the pytest command:

```
pytest test_integration.py
```
## Contributing
If you'd like to contribute to this project, feel free to fork the repository and submit a pull request. Please make sure to follow the existing code style and test coverage guidelines.

        
