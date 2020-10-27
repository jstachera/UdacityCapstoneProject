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

The dags folder contain the ETL process which is run on the monthly basis.
<br/>The process_* tasks are executing the SparkSubmitOperator.
<br/>The load_* tasks are executing custom StageToRedshiftOperator which is defined in plugins/operators folder.
<br/>The create_img_db.sql is the final star schema SQL definition file which is run on the Redshfit cluster prior to ETL to create required tables.
<br/>The /tests folder contains the unit tests.
<br/>The /tests/i94project subfolder contains the local input data for the ETL process unit testing (see folder structure below).

# Input data
<br/>The ETL process operates on the following input data structure :

<br/>Amazon S3/
<br/>└──i94project
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
