# US Immigration Analysis - Data Engineering Capstone Project  
  
## 1. Project Scope 
### Main goal
Implement the ETL process which will be executed on a regular basis and will be responsible for cleaning, extracting and loading the data for later use in the business analysis.
Final data model can be used for verifying the correlation between:
 - destination temperature and immigration statistics
 - destination in the U.S and the source country
 - destination in the U.S and the source climates
 - arrival month and number of immigrants
### Data
The project is based on the immigration dataset as a primary dataset and supplementary datasets like demographics, temperatures and aircodes.
### End solution
The end solution will make use of the Airflow workflow system which will call all the ETL stages on monthly basis.
For processing (cleaning/transforming) the immigration data there will be used the Apache Spark.
The Apache Spark output will be saved into the S3 buckets.
Finally, the saved data will be loaded into the Redshift cluster for the business analytics queries.

![ETL Architecture](https://lucid.app/publicSegments/view/9943301e-f97d-4cfa-b0df-0ee1a3ec45ab/image.png)

##  2. Datasets

### Description
There are four datasets:
 - **Immigration to the United States** ([source](https://travel.trade.gov/research/reports/i94/historical/2016.html))
 - U.S. city demographics ([source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/))
 - Airport codes ([source](https://datahub.io/core/airport-codes#data))
 - Temperatures ([source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data))

The main dataset is the immigration to the United States, and the rest are supplementary datasets.

#### Diagaram
Simplified diagram showing main datasets features.
![enter image description here](https://app.lucidchart.com/publicSegments/view/b22781a6-a7e7-4a0e-8a24-0c1fbabe12c7/image.png)

### Data source
The datasets for this project come from the UDacity provided resources.

### Data definition

#### Imigration dataset
This dataset comes from the US National Tourism and Trade Office. An I-**94** known as the Arrival-Departure Record Card is confirmation that a foreign national was allowed entry into the country. It keeps track of during any entries into the US in any nonimmigrant visa status.
This dataset contains the following information:
                                                 
| Column   | Description                                                                                      |
|----------|--------------------------------------------------------------------------------------------------|
| CICID    | CICID is a unique number for the immigrants                                                      |
| I94YR    | 4 digit year                                                                                     |
| I94MON   | Numeric month                                                                                    |
| I94CIT   | country of citizenship                                                                           |
| I94RES   | country of residence                                                                             |
| I94PORT  | 3 character code of US city destination                                                          |
| ARRDATE  | Arrival date in the US                                                                           |
| I94MODE  | 1 digit travel code (1 = 'Air', 2 = 'Sea', 3 = 'Land', 9 = 'Not reported')                       |
| I94ADDR  | Address where the immigrants resides in US state                                                 |
| DEPDATE  | Departure Date from the US                                                                       |
| I94BIR   | Respondent age  in Years                                                                         |
| I94VISA  | Visa codes                                                                                       |
| COUNT    | Used for summary statistics                                                                      |
| DTADFILE | Character Date Field - Date added to I-94 Files - CIC does not use                               |
| VISAPOST | Department of State where where Visa was issued - CIC does not use                               |
| OCCUP    | Occupation that will be performed in US  - CIC does not use                                      |
| ENTDEPA  | Arrival Flag - admitted or paroled into the US - CIC does not use                                |
| ENTDEPD  | Departure Flag - Departed, lost I-94 or is deceased - CIC does not use                           |
| ENTDEPU  | Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use      |
| MATFLAG  | Match flag - Match of arrival and departure records                                              |
| BIRYEAR  | 4 digit year of birth                                                                            |
| DTADDTO  | Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use |
| GENDER   | Non-immigrant sex                                                                                |
| INSNUM   | INS number                                                                                       |
| AIRLINE  | Airline used to arrive in U.S                                                                    |
| ADMNUM   | Admission number                                                                                 |
| FLTNO    | Flight number of airline used to arrive in US                                                    |
| VISATYPE | Type of visa which one owns                                                                      |
#### Demographics dataset
This dataset consist of data about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.
This dataset contains following information:
- City
- State
- Median Age
- Male Population
- Female Population
- Total Population
- Number of Veterans
- Foreign-born
- Average Household Size
- State Code
- Race
- Count (race population count in the given city)

#### Airport dataset
This dataset contains following information:
- ident
- type
- name
- elevation_ft
- continent
- iso_country
- iso_region
- municipality
- gps_code
- iata_code
- local_code
- coordinates


#### Temperature dataset
Historical temperature dataset starting from the year 1750.
This dataset contains following information:
- Date
- AverageTemperature
- AverageTemperatureUncertainty
- City
- Country
- Latitude
- Longitude
#### Embassies dataset
The list of U.S. embassies and consulates.
This dataset contains following information:
- code
- city
- country

#### Dictionaries
On the basis of the provided SAS description file: I94_SAS_Labels_Descriptions.SAS, following dictionaries of valid values were extracted:
 - Country codes (3 digits country code, country name)
 - U.S. ports (3 letters port code, port name, 2 letters state code)
 - U.S. states (2 letters state code, state name)
 - Visa modes (1 digit mode number, mode name)
 - Arrival types (1 digit arrival type number, arrival type name)

## How to run
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
8. Open the airflow UI:
```console
http://localhost:8080/
```
9. Setup the connections:
- AWS Credentials
- AWS Redshift
- Apache Spark (Host: local, Extra: {"queue": "root.default","spark-home":"/opt/spark"})
	
10. Run the DAG.
11. Verify the logs.
