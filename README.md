# ATM Data Transaction Analysis using complete ETL Process for Apache Sqoop, Spark and Amazon Redshift.
This is complete Extract, Transform, Load process used for analysis of ATM Hotspots.
Spar Nord Bank is trying to observe the withdrawal behaviour and the corresponding dependent factors to optimally manage the refill frequency.
Apart from this, other insights also can be drawn from the data. The dataset used is available at the location: https://www.kaggle.com/sparnord/danish-atm-transactions

This data set contains various types of transactional data as well as the weather data at the time of the transaction, such as:
- Transaction Date and Time: Year, month, day, weekday, hour
- Status of the ATM: Active or inactive
- Details of the ATM: ATM ID, manufacturer name along with location details such as longitude, latitude, street name, street number and zip code
- The weather of the area near the ATM during the transaction: Location of measurement such as longitude, latitude, city name along with the type of weather, temperature, pressure, wind speed, cloud and so on
- Transaction details: Card type, currency, transaction/service type, transaction amount and error message (if any)


Below are the steps that are performed in the ETL Process:
- Extracting the transactional data from a given MySQL RDS server to HDFS(EC2) instance using Sqoop.
- Transforming the transactional data according to the given target schema using PySpark. 
- This transformed data is to be loaded to an S3 bucket.
- Creating the Redshift tables according to the given schema.
- Loading the data from Amazon S3 to Redshift tables.
- Performing the analysis queries.


## 1. Sqoop Job Created to Import the data into HDFS:

**sqoop job --create bank_data_import -- import \
--connect jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase \
--table SRC_ATM_TRANS \
--username student --password STUDENT123 \
--target-dir /user/root/ETL_Project/bank_data_import \
--fields-terminated-by ',' --lines-terminated-by '\n' \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
--null-string '\\N' --null-non-string '\\N' \
-m 1**

## 2. The process of data transformation is present in the PySpark File.

## 3. Amazon Redshift Schema Creation:

**create group spark_redshift_etl_group with user awsuser;**

**create schema if not exists etl_bank_schema;**

**create table if not exists etl_bank_schema.dim_location(
  location_id varchar(50) not null distkey sortkey primary key,
  location varchar(50),
  streetname varchar(255),
  street_number integer,
  zipcode integer,
  lat decimal(10,3),
  lon decimal(10,3)
);**

**create table if not exists etl_bank_schema.dim_card_type(
  card_type_id varchar(50) not null distkey sortkey primary key,
  card_type varchar(40)
);**

**create table if not exists etl_bank_schema.dim_date(
  date_id varchar(50) not null distkey sortkey primary key,
  full_date_time timestamp,
  year integer,
  month varchar(20),
  day integer,
  hour integer,
  weekday varchar(20)
);**

**create table if not exists etl_bank_schema.dim_atm(
  atm_id varchar(50) not null distkey sortkey primary key,
  atm_number varchar(20),
  atm_manufacturer varchar(50),
  atm_location_id varchar(50) references etl_bank_schema.dim_location(location_id)
);**

**create table if not exists etl_bank_schema.fact_atm_trans(
  trans_id varchar(50) not null distkey sortkey primary key,
  atm_id varchar(50) references etl_bank_schema.dim_atm(atm_id),
  weather_loc_id varchar(50) references etl_bank_schema.dim_location(location_id),
  date_id varchar(50) references etl_bank_schema.dim_date(date_id),
  card_type_id varchar(50) references etl_bank_schema.dim_card_type(card_type_id),
  atm_status varchar(20),
  currency varchar(10),
  service varchar(20),
  transaction_amount integer,
  message_code varchar(255),
  message_text varchar(255),
  rain_3h decimal(10,3),
  clouds_all integer,
  weather_id integer,
  weather_main varchar(50),
  weather_description varchar(255)
);**


**copy etl_bank_schema.dim_location from
's3_location//dim_location/dim_location.csv'
iam_role 'aws_iam_role'
csv  region 'us-east-1';**

**copy etl_bank_schema.dim_atm from
's3_location//dim_atm/dim_atm.csv'
iam_role 'aws_iam_role'
csv  region 'us-east-1';**

**copy etl_bank_schema.dim_card_type from
's3_location//dim_card_type/dim_card_type.csv'
iam_role 'aws_iam_role'
acceptinvchars csv  region 'us-east-1';**

**copy etl_bank_schema.dim_date from
's3_location//dim_date/dim_date.parquet'
iam_role 'aws_iam_role'
format as parquet;**

**copy etl_bank_schema.fact_atm_trans from
's3_location//fact_transaction/fact_transaction.csv'
iam_role 'aws_iam_role'
csv  region 'us-east-1';**


## 4. Redshift Queries are shared in the document. Below are the questions considered for the analysis:
<br>a) Top 10 ATMs where most transactions are in the ’inactive’ state.
<br>b) Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions.
<br>c) Top 10 ATMs with the most number of transactions throughout the year.
<br>d) Number of overall ATM transactions going inactive per month for each month.
<br>e) Top 10 ATMs with the highest total amount withdrawn throughout the year.
<br>f) Number of failed ATM transactions across various card types.
<br>g) Top 10 records with the number of transactions ordered by the ATM_number, ATM_manufacturer, location, weekend_flag and then total_transaction_count, on weekdays and on weekends throughout the year.
<br>h) Most active day in each ATMs from location "Vejgaard".
