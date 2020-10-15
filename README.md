# technical_challenge
DE Technical Challenge Marko Gogic

The goal of this challenge was to create a data pipeline to load and transform the dataset and to create a dimensional data model by using Python and SQL. 

The data set consisted of a single excel file containing 19,012 rows and 5 columns: event_id, event_type, professional_id_anonymized, created_at, and meta_data.

Python 'Pandas' and 'SqlAlchemy' packages were used to load the excel file and to write it into the MySql database. The data was written into the staging table named 'event_log'. 
As no primary key constraint was applied, duplicate rows were possible at this step. 

Connection to the database was created with the use of 'mysql-connector-python' package, after which SQL queries were used to create new tables, pull the data from the database, transform the data and write it back
in the newly created tables. 

Aside from the initial staging table, 4 new tables were created:

1. availability_snapshot: datum DATE NOT NULL, active_pros INT(11) - containing the number of active professionals per day

2. event_dim: id INT(11) NOT NULL AUTO_INCREMENT, event VARCHAR(55) - containing different event types (4 in total)

3. fact_table: event_id INT(11) NOT NULL,event_type_id INT(11) NOT NULL, professional_id_anonymized INT(11) NOT NULL, created_at DATETIME NOT NULL, 
service_id INT(11), price DECIMAL(7,2) - containing the same data as the initial data set but with 'service_id' and 'price' extracted from the metadata; primary key
constraint was applied to the fact table in order to filter out any duplicate values with the same event_id. 

4. service_dim: id INT(11) NOT NULL, service_dutch_name VARCHAR(255), service_english_name VARCHAR(255) - containing data about service types extracted from the meta_data columns




Docker instructions: in Powershell go to the working directory and run the "docker-compose up" command. 
Docker-compose will first start the 'mysql-db' container and will wait for the MySql Server to start before running the Python container. 
It usually takes a couple of minutes for the MySql server to get up and running, and in case that the connection fails, the timeout variable in the docker-compose file 
which is currently set to 40s should be set to a higher value.


