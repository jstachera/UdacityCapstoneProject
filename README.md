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

### Data definition
Data definition and EDA (exploratory data analysis) is placed in the /jupyter folder.

### ETL Implementation
ETL process implementation is placed in the /airflow folder.
