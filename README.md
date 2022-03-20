# Sparkify Airflow ETL

## Motivation

The startup, Sparkify, is a music streaming app and wants to analyse what songs their users are listening to. Their current data is available as JSON files on AWS S3 and cannot be easily analised. The purpose of the ETL process is to extract the JSON files from S3, transform the data into fact and dimension tables in RedShift. Apache Airflow is used to extract the data on an hour basis.

## Data model

Data was normalized into a star schema with fact and dimention tables in order to use data for the analysis but also reuse data for future analysis.

## File descriptions

- `dag\sparkify_dag.py`: Apache Airflow DAG python file.
- `plugins\helpers\sql_queries.py`: SQL queries to create tables, select data for inserting in the data model and tests.
- `plugins\operators\stage_redshift.py`: Custom StageToRedshiftOperator for copying files from S3 to RedShift staging.
- `plugins\operators\load_fact.py`: Custom LoadFactOperator to create fact table and insert data.
- `plugins\operators\load_dimension.py`: Custom LoadDimensionOperator to create dimension tables and insert data.
- `plugins\operators\data_quality.py`: Custom DataQualityOperator to check that rows have been inserted and checking for NULLs.

## How to run the files and notebooks

Dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

## License

GNU GPL v3

## Author

Coenraad Pretorius
