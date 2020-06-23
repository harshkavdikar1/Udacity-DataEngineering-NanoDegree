# Data Engineering Capstone Project

## Scope of Works
The purpose of this project is to demonstrate various skills associated with data engineering projects. In particular, developing ETL pipelines using Airflow, constructing data warehouses through Redshift databases and S3 data storage as well as defining efficient data models e.g. star schema. As an example I will perform pipeline of US immigration, primarily focusing on the type of visas being issued and the profiles associated. The scope of this project is limited to the data sources listed below with data being aggregated across numerous dimensions.


## Data Description & Sources
- I94 Immigration Data: This data comes from the US National Tourism and Trade Office found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

- U.S. City Demographic Data: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. Dataset comes from OpenSoft found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

- Airport Code Table: This is a simple table of airport codes and corresponding cities. The airport codes may refer to either IATA airport code, a three-letter code which is used in passenger reservation, ticketing and baggage-handling systems, or the ICAO airport code which is a four letter code used by ATC systems and for airports that do not have an IATA airport code (from wikipedia). It comes from [here](https://datahub.io/core/airport-codes#data).

After extracting various immigration codes from the  `I94_SAS_Labels_Descriptions.SAS` file, I was able to define a star schema by extracting the immigration fact table and various dimension tables as shown below:


## Data Model
![image](./images/ER.png)

### **Data Dictionary Dimension Tables**

#### airport_codes (Airports Data)

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| ident | VARCHAR | Airport id
| type | VARCHAR | Size of airport
| name | VARCHAR | name
| elevation_ft | float | Elevation in feet
| continent | VARCHAR  | Continet
| iso_country |  VARCHAR  | Country (ISO-2)
| iso_region | VARCHAR | region (ISO-2)
| municipality | VARCHAR | Municipality
| gps_code | VARCHAR | GPS code
| iata_code | VARCHAR | IATA code
| local_code | VARCHAR | Local code
| coordinates | VARCHAR  |  Coordinates
 
 #### i94cit_i94res (Countries)

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR  | Country code
 | reason_for_travel |  VARCHAR | Country
 
 
 #### i94port (Entry Airport )

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 | code | VARCHAR | Entry airport code
 | port_of_entry | VARCHAR | Airport city and state
 | city | VARCHAR | Airport city
 |  state_or_country | VARCHAR | Airport state or country
 
 #### i94addr (Entry State)

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 |code | VARCHAR  | State code
 | state |  VARCHAR | State
 
 #### i94visa (Visa)

 | Attribute      | Type    | Description     |
 | ---------- | :-----------:  | :-----------: |
 |code | VARCHAR  | Visa code
 | reason_for_travel |  VARCHAR | Visa description
 
 #### i94mode (Mode to access)

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
|code | VARCHAR  | Transportation code
| transportation |  VARCHAR | Transportation description

#### us_state_race (Race of US state)

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| state_code | VARCHAR | State Code
| state | VARCHAR | State
| race | VARCHAR | Race type
| race_ratio | FLOAT | Race percent

#### us_cities (U.S. Demographicy by city)
| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| city         |               VARCHAR | City
| state        |              VARCHAR | State
| median_age |                FLOAT | Median of age
| male_population |            FLOAT | Number of male population
| female_Population |          FLOAT | Number of female population
| total_Population     |      FLOAT | Number of total population
| number_veterans    |        FLOAT | Number of veterans
| foreign_born     |          FLOAT | Number of foreign born 
| average_household_size |    FLOAT | Average household size
|state_code         |        VARCHAR | State Code

### Fact Table 
#### immigration (Immigration Registry)

| Attribute      | Type    | Description     |
| ---------- | :-----------:  | :-----------: |
| cicid | FLOAT | CIC id | 
| i94yr | FLOAT | 4 digit year  |
| i94mon |FLOAT | Numeric month | 
| i94cit| FLOAT | City |
| i94res | FLOAT | Country code |
|  i94port | VARCHAR | Airport code |
| arrdate  | FLOAT | Arrival Date in the USA |
| i94mode  | FLOAT | Mode to access |
| i94addr  |VARCHAR | State code |
|  depdate | FLOAT | Departure Date from the USA |
|  i94bir  | FLOAT | Age |
|  i94visa | FLOAT | Vias code |
|  count  | FLOAT | Used for summary statistics |
|  dtadfile | VARCHAR |  Character Date Field - Date added to I-94 Files | 
|  visapost | VARCHAR | Department of State where where Visa was issued |
|  occup | VARCHAR |  Occupation that will be performed in U.S. |
| entdepa | VARCHAR | Arrival Flag - admitted or paroled into the U.S.|
| entdepd | VARCHAR | Departure Flag - Departed, lost I-94 or is deceased 
| entdepu  | VARCHAR | Update Flag - Either apprehended, overstayed, adjusted to perm residence |
|  matflag  | VARCHAR | Match flag - Match of arrival and departure records |
|  biryear | FLOAT | 4 digit year of birth |
|  dtaddto | VARCHAR | Date to which admitted to U.S. |
| gender | VARCHAR | Non-immigrant sex |
| insnum | VARCHAR | INS number |
| airline | VARCHAR | Airline used to arrive in U.S. | 
| admnum | FLOAT | Admission Number |
| fltno | VARCHAR | Flight number of Airline used to arrive in U.S. |
| visatype | VARCHAR  | Class of admission legally admitting the non-immigrant to temporarily stay in U.S. |
 

## Data Storage

Data was stored in S3 buckets in a collection of CSV and PARQUET files. The immigration dataset extends to several million rows and thus this dataset was converted to PARQUET files to allow for easy data manipulation and processing through Dask and the ability to write to Redshift.


## ETL Pipeline

Defining the data model and creating the star schema involves various steps, made significantly easier through the use of Airflow. The process of extracting files from S3 buckets, transforming the data and then writing CSV and PARQUET files to Redshift is accomplished through various tasks highlighted below in the ETL Dag graph. These steps include:
- Extracting data from SAS Documents and writing as CSV files to S3 immigration bucket
- Extracting remaining CSV and PARQUET files from S3 immigration bucket
- Writing CSV and PARQUET files from S3 to Redshift
- Performing data quality checks on the newly created tables

![image](./images/etl_process.png)

## Conclusion
Overall this project was a small undertaking to demonstrate the steps involved in developing a data warehouse that is easily scalable. Skills include:
* Creating a Redshift Cluster, IAM Roles, Security groups.
* Developing an ETL Pipeline that copies data from S3 buckets into staging tables to be processed into a star schema
* Developing a star schema with optimization to specific queries required by the data analytics team.
* Using Airflow to automate ETL pipelines using Airflow, Python, Amazon Redshift.
* Writing custom operators to perform tasks such as staging data, filling the data warehouse, and validation through data quality checks.
* Transforming data from various sources into a star schema optimized for the analytics team's use cases.
