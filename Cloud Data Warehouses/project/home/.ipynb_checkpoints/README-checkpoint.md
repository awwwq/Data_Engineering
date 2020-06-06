### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to and need an efficient way to query the data.

This project intend to create a proper Postgres database and ETL pipeline for Sparkift's analysis .



#### Database schema

![](ER.png)



#### ETL process

Extract data from original json file storing in AWS S3 and insert it into staging tables

--->Transform data then insert it into star schema tables 




####  File structure

1. `dwh.cfg` redshift database and IAM role info.
2. `create_tables.py` drops and creates tables.  Run this file to reset tables before each time you run your ETL scripts.
3. `etl.py` load data from `song_data` and `log_data` into staging tables then transform data and load into final tables. 
4. `sql_queries.py` contains all sql queries, and is imported into the last two files above.
5. AWS S3 bucket contains song_data and log_data



#### How to use

drop and create tables

```
python create_tables.py
```

process data and insert it to database

```
python etl.py
```