# Cloud Data Warehouse with AWS

#### Introduction and Motivation
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The created architechture should serve the purpose of serving queries raised by Sparkify's analytics team.

#### Tasks at hand
Build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

#### Available data
Two datasets are available - the song dataset and the log dataset. Both datasets contain multiple files are in JSON file format. The song dataset is a subset of real data from the Million Song Dataset and each file in the dataset contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. The log dataset consists of log files a event simulator based on the songs in the song dataset. 

#### Files in the repository

- data folder : This folder contains the data for building the tables and the database. Inside it contains the two datasets: the song dataset and the log dataset in two folders

- create_tables.py : This python script contains relevant functions to create, drop, connect to databases and tables. This file to reset the tables before running the ELT scirpts each time.

- etl.py : Reads and processes ALL the files from song dataset and log dataset and loads them into your tables. 

- sql_queries.py : This contains all the sql queries, and is imported into the last three files above.

- dwh.cfg : Configuration file that contains the links, access ids for AWS

- README.md : Description of the project and the repository

#### How to run the scripts

1. First execute the command for creating tables and connecting to the database `python3 create_tables.py`.
2. Then execute this command to start the ETL process to load the data into the database `python3 etl.py`.

#### Database Schema design
First we load the data from the two datasets into two Staging stables in S3. They have the same design as the two datasets.
Then, we have utilized a star schema design for the Redshift database tables that is ideal for analytical query handling but also for data integrity. With star schema, the queries from analytical team can be handled with both high efficiency and performance. The fact table and the dimentsion tables are listed below with their corresponding columns. The first column in the list is the PRIMARY KEY for the corresponding table.

###### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables

- users : users in the app
    - user_id, first_name, last_name, gender, level

- songs : songs in music database
    - song_id, title, artist_id, year, duration

- artists : artists in music database
    - artist_id, name, location, latitude, longitude

- time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

#### ETL Pipeline
The ETL pipeline actively segregates tasks pertaining to creating tables, loading tables, sql queries to run the previous two tasks and the configuration crednetials required for the tasks. Once the tables (Staging and Redshift ) are created and Redshift cluster is initialized, the ETL process in the file etl.py is carried out in the following steps:

1. Connection to the intialized AWS staging and Redshift cluster is established.
2. List of all files the two datasets are obtained, each JSON is read and copied to corresponding S3 Staging tables first.
2. Information pertaining to each table in the Star schema design is obtained, pre-processed and inserted into the tables.
    1. The information pertaining to song metadata is entered into the **songs** table
    2. The information pertaining to artists for each song is entered into the **artists** table
    1. The timestamp is converted into the unix format and broken down to insert into the **time** table
    2. The information pertaining to the users from the log file is entered into the **users** table
    3. The log information about all songs accessed by users is inserted into **songplays** table
4. Disconnect from database.

#### Example queries
1. Find all songs titles played by paid users so that aggrement with those labels can be renewed: 
    SELECT DISTINCT songs.title 
    FROM songs 
    JOIN songplays ON songplays.song_id = songs.song_id 
    WHERE songplays.level = "paid"


