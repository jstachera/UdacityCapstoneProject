# Pipeline implementation
The data pipeline was implemented in the Apache airflow with the following project structure:

<br/>airflow/
<br/>├── dags/
<br/>├── plugins/
<br/>│   ├── helpers/
<br/>│   └── operators/
<br/>└── tests/
<br/>│   ├── i94project
<br/>│   ├── tools
<br/>│   └── test_*.py
<br/>└── create_img_db.sql
<br/>└── README.MD

## DAG
The dags folder contain the ETL process which is run on the monthly basis.
<br/>The process_* tasks are executing the SparkSubmitOperator.
<br/>The load_* tasks are executing custom StageToRedshiftOperator which is defined in plugins/operators folder.
<br/>The create_img_db.sql is the final star schema SQL definition file which is run on the Redshfit cluster prior to ETL to create required tables.

![ETL Process](https://app.lucidchart.com/publicSegments/view/429f080b-5aac-4b2e-acbd-58edf622c70b/image.png "ETL Process")

## Tests
<br/>The /tests folder contains the unit tests.
<br/>The /tests/i94project subfolder contains the local input data for the ETL process unit testing (see folder structure below).

# Input data
<br/>The ETL process operates on the following input data structure :

i94project
<br/>├── stage
<br/>│   └── input
<br/>│   └── output
<br/>└── dicts

The stage/input contains all the data that are processed by process_* tasks:
 * i94 immigration data (file per month)
 * airport codes
 * global temperatures
 * us cities demographics

The dicts folder contains all the required dictionaries:
 * arrival type
 * country code
 * us port
 * us state
 * visa mode
 * visa type
 
## How to run
The ETL implementation is placed in the /airflow folder.
In order to run the ETL process following steps must be done:

1. Create AWS Redshift Cluster.
2. Run the 'create_img_db.sql' int the created cluster.
3. Install docker locally.
4. Pull the Airflow docker image:
```console
docker pull puckel/docker-airflow
```
5. Run the docker with mapped local folders for airflow dag and plugin folder:
```console
docker run -d -p 8080:8080 -e FERNET_KEY="SOME_GENERATED_KEY"  -v d:/airflow/dags/:/usr/local/airflow/dags -v d:/airflow/plugins/:/usr/local/airflow/plugins  puckel/docker-airflow:spark webserver
```
6. Install the Apache Spark 3.0.1 according to:
- https://phoenixnap.com/kb/install-spark-on-ubuntu
7. Open the airflow UI:
```console
http://localhost:8080/
```
8. Setup the connections:
- AWS Credentials
- AWS Redshift
- Apache Spark (Host: local, Extra: {"queue": "root.default","spark-home":"/opt/spark"})
	
9. Run the DAG.
10. Verify the logs.
