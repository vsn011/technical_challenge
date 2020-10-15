import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
import os
import yaml
import glob
import mysql.connector



#preparing connection and inserting initial data into the database
variables = yaml.load(open('/opt/events.yaml'))
path = variables['path']
schema = variables['schema']
table = variables['table']
user = variables['mysql_user1']
user2 = variables['mysql_user2']
password = variables['mysql_password']
host = variables['mysql_host']


#setting up the directory and getting the list of the files
extension = 'xlsx'
os.chdir(path)
files = glob.glob('*.{}'.format(extension))
print(files)

dataframes = []
for f in files:
    df = pd.read_excel(f)
    dataframes.append(df)


#db = MySQLdb.connect(host, user, password, auth_plugin='mysql_native_password')
db = mysql.connector.connect(user=user, password=password, host=host, auth_plugin='mysql_native_password')
cursor = db.cursor()

#creating database
cursor.execute("SET sql_notes = 0; ")
cursor.execute("CREATE DATABASE IF NOT EXISTS {0};".format(schema))
cursor.execute("SET sql_notes = 1; ")

#creating new user
cursor.execute("SET sql_notes = 0; ")
cursor.execute("CREATE USER IF NOT EXISTS '{0}'@'%' IDENTIFIED WITH mysql_native_password BY '{1}';".format(user2,password))
cursor.execute("SET sql_notes = 1; ")

cursor.execute("SET sql_notes = 0; ")
cursor.execute("GRANT ALL ON {0}.* TO '{1}'@'%';".format(schema,user2))
cursor.execute("SET sql_notes = 1; ")

#df = pd.read_excel('event_log.xlsx')
#insterting data into the staging table where duplicates are possible, no PK constraint
#engine = create_engine('mysql://{1}:{2}@{3}/{0}'.format(schema,user,password,host))
engine = create_engine("mysql+mysqlconnector://{1}:{2}@{3}/{0}?auth_plugin=mysql_native_password".format(schema,user,password,host))


for d in dataframes:
    d.to_sql(table, con=engine, if_exists='append')


db = MySQLdb.connect(host, user2, password)
cursor = db.cursor()
#creating data model from the initial table
#event_dim table
cursor.execute("SET sql_notes = 0; ")
cursor.execute("CREATE TABLE IF NOT EXISTS {0}.events_dim (id INT(11) NOT NULL AUTO_INCREMENT, event VARCHAR(55), PRIMARY KEY (id));".format(schema))
cursor.execute("SET sql_notes = 1; ")
cursor.execute("""INSERT INTO {0}.events_dim (event) SELECT DISTINCT t.event_type FROM {0}.{1} t
WHERE t.event_type NOT IN (SELECT DISTINCT event FROM {0}.events_dim);""".format(schema, table))
cursor.close()
cursor = db.cursor()
db.commit()

#service dimension table
cursor.execute("SET sql_notes = 0; ")
cursor.execute("""CREATE TABLE IF NOT EXISTS {0}.service_dim (id INT(11) NOT NULL,
service_dutch_name VARCHAR(255), service_english_name VARCHAR(255), PRIMARY KEY (id));""".format(schema))
cursor.execute("SET sql_notes = 1; ")

cursor.execute("""INSERT INTO {0}.service_dim (id, service_dutch_name, service_english_name)
 SELECT DISTINCT SUBSTRING_INDEX(meta_data, '_', 1) as id, SUBSTRING_INDEX(SUBSTRING_INDEX(meta_data, '_', 2), '_', -1) as service_dutch_name,
SUBSTRING_INDEX(SUBSTRING_INDEX(meta_data, '_', 3), '_', -1) as service_english_name
FROM {0}.{1} WHERE meta_data IS NOT NULL
ON DUPLICATE KEY UPDATE service_dutch_name=SUBSTRING_INDEX(SUBSTRING_INDEX(meta_data, '_', 2), '_', -1);""".format(schema, table))
cursor.close()
cursor = db.cursor()
db.commit()

#availability snapshot table
cursor.execute("SET sql_notes = 0; ")
cursor.execute("CREATE TABLE IF NOT EXISTS {0}.availability_snapshot (datum DATE NOT NULL, active_pros INT(11), PRIMARY KEY (datum));".format(schema))
cursor.execute("SET sql_notes = 1; ")

cursor.execute("""INSERT INTO {0}.availability_snapshot (datum, active_pros)
with data as (select date(created_at) as datum,
sum(case when event_type = 'became_able_to_propose' then 1
when event_type = 'became_unable_to_propose' then -1
else 0 end) new_active,
min(date(created_at)) as min_datum from {0}.{1}
group by date(created_at)
order by datum asc)

SELECT datum, sum(new_active) over(order by datum) as active_pros FROM data
WHERE datum BETWEEN min_datum and '2020-03-10'
GROUP BY datum
ON DUPLICATE KEY UPDATE datum=datum;""".format(schema, table))
cursor.close()
cursor = db.cursor()
db.commit()

#fact table - derived from the staging table but with PK constraint in order to prevent duplicates
cursor.execute("SET sql_notes = 0; ")
cursor.execute("""CREATE TABLE IF NOT EXISTS {0}.fact_table (event_id INT(11) NOT NULL,
event_type_id INT(11) NOT NULL, professional_id_anonymized INT(11) NOT NULL,
created_at DATETIME NOT NULL, service_id INT(11), price DECIMAL(7,2), PRIMARY KEY (event_id));""".format(schema))
cursor.execute("SET sql_notes = 1; ")

cursor.execute("""INSERT INTO {0}.fact_table (event_id, event_type_id, professional_id_anonymized, created_at, service_id, price)
SELECT
l.event_id, e.id as event_type_id, l.professional_id_anonymized, l.created_at,
SUBSTRING_INDEX(l.meta_data, '_', 1) as service_id,
FORMAT(SUBSTRING_INDEX(SUBSTRING_INDEX(l.meta_data, '_', 4), '_', -1), 2) as price
FROM {0}.event_log l
LEFT JOIN {0}.events_dim e ON l.event_type = e.event
ON DUPLICATE KEY UPDATE created_at=l.created_at;
 """.format(schema, table))
cursor.close()
cursor = db.cursor()
db.commit()
