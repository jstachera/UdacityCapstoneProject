/* Drop fact table */

SELECT 'Dropping old tables...' as Dropping;

DROP TABLE IF EXISTS "fact_immigration";

/* Drop dimension tables */

DROP TABLE IF EXISTS "dim_arrival_type";
DROP TABLE IF EXISTS "dim_visa_type";
DROP TABLE IF EXISTS "dim_visa_mode";
DROP TABLE IF EXISTS "dim_date";
DROP TABLE IF EXISTS "dim_country";
DROP TABLE IF EXISTS "dim_us_airport";
DROP TABLE IF EXISTS "dim_us_state";

/* Create dimension tables */

SELECT 'Creating new tables...' as Creating;

CREATE TABLE "dim_arrival_type" (
  "id" SMALLINT NOT NULL PRIMARY KEY,
  "name" NVARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE "dim_visa_type" (
  "code" CHAR(3) NOT NULL PRIMARY KEY,
  "mode" NVARCHAR(50)  NOT NULL,
  "description" NVARCHAR(255) NOT NULL
);

CREATE TABLE "dim_visa_mode" (
  "id" SMALLINT NOT NULL PRIMARY KEY ,
  "name" NVARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE "dim_date" (
  "date" DATE NOT NULL PRIMARY KEY,
  "year" SMALLINT NOT NULL,
  "quarter" SMALLINT NOT NULL,
  "month" SMALLINT NOT NULL,
  "day_of_week" SMALLINT NOT NULL,
  "day_of_month" SMALLINT NOT NULL,
  "day_of_year" SMALLINT NOT NULL,
  "week_of_year" SMALLINT NOT NULL
);

CREATE TABLE "dim_country" (
  "code" CHAR(3) NOT NULL PRIMARY KEY,
  "name" NVARCHAR(100) NOT NULL UNIQUE,
  "avg_temp_cels" FLOAT
);

CREATE TABLE "dim_us_airport" (
  "local_code" CHAR(5) NOT NULL PRIMARY KEY,
  "type" NVARCHAR(50) NOT NULL,
  "name" NVARCHAR(300) NOT NULL,
  "elevation_ft" FLOAT,
  "us_state_code" CHAR(3) NOT NULL,
  "municipality" NVARCHAR(100),
  "coordinates" NVARCHAR(200)
);

CREATE TABLE "dim_us_state" (
	"us_state_code" CHAR(3) NOT NULL PRIMARY KEY,
	"us_state_name" NVARCHAR(100) NOT NULL UNIQUE,
	"male_count" INT,
	"female_count" INT,
	"total_population" BIGINT,
	"veteran_count" INT,
	"foreigner_count" INT,
	"indian_count" INT,
	"asian_count" INT,
	"black_count" INT,
	"hispanic_count" INT,
	"white_count" INT,
	"male_pct" decimal(5,2),
	"female_pct" decimal(5,2),
	"veteran_pct" decimal(5,2),
	"foreigner_pct" decimal(5,2),
	"indian_pct" decimal(5,2),
	"asian_pct" decimal(5,2),
	"black_pct" decimal(5,2),
	"hispanic_pct" decimal(5,2),
	"white_pct" decimal(5,2),
  "median_age" float,
  "avg_household_size" float
);

/* Create fact table */

CREATE TABLE "fact_immigration" (
  "id" BIGINT NOT NULL PRIMARY KEY,
  "report_year" SMALLINT NOT NULL,
  "report_month" SMALLINT NOT NULL,
  "birth_country_code" CHAR(3) REFERENCES dim_country(code),
  "residence_country_code" CHAR(3) REFERENCES dim_country(code),
  "gender" CHAR(1),
  "birth_year" SMALLINT,
  "person_age" SMALLINT,
  "admission_number" BIGINT,
  "added_date" DATE REFERENCES dim_date(date),
  "us_airport_code" CHAR(5) REFERENCES dim_us_airport(local_code),
  "us_state_code" CHAR(3) REFERENCES dim_us_state(us_state_code),
  "arrival_date" DATE REFERENCES dim_date(date),
  "arrival_type_id" SMALLINT REFERENCES dim_arrival_type(id),
  "visa_mode_id" SMALLINT REFERENCES dim_visa_mode(id),
  "visa_type_code" CHAR(3) REFERENCES dim_visa_type(code),
  "airline_code" NVARCHAR(10),
  "flight_number" NVARCHAR(10),
  "allowed_stay_date" DATE REFERENCES dim_date("date"),
  "departure_date" DATE REFERENCES dim_date("date"),
  "stay_days" INT
);

SELECT 'Finished creating immigration db' as Finished