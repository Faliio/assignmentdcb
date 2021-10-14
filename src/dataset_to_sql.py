from os import path
from os import environ
import sys
import requests
import json
import pandas as pd
import urllib
from sqlalchemy import create_engine

#conn_db_url = 'db.sqlite'
#table_name = 'air_quality'
#url = "https://data.cdc.gov/api/views/cjae-szjv/rows.json?accessType=DOWNLOAD"
#file_path = 'rows.json'


url = environ['URL']
table_name = environ['TABLE']
db_url = environ['DB_HOST']
db_user = environ['DB_USER']
db_pass = environ['DB_PASS']
db_name = environ['DB_NAME']


print('******** # 1st part - Ingest data into a relational database from JSON files ********')

# Open remote JSON file
f = urllib.request.urlopen(url)

# Open local JSON file
#f = open(file_path, 'r')

# Load JSON data
data = json.load(f)

f.close()

# Get column name list from data->meta->view->columns
columns_list = [column['name'] for column in data['meta']['view']['columns']]
print('\nColumn list ', columns_list)
# Get row list
rows_list = data['data']

# creating DataFrame from JSON data
df = pd.DataFrame(rows_list, columns=columns_list)

# Get numeric column
numeric_columns = [column['name'] for column in filter(
    lambda col: col['dataTypeName'] == 'number', data['meta']['view']['columns'])]

# Infer float type in DataFrame numeric_columns
for col in numeric_columns:
    df[col] = df[col].astype('float')

print('\nInfo loaded DataFrame\n', df.info())

print('\nDescribe loaded DataFrame\n', df.describe())

# SQL connection
engine = create_engine(
    'mysql+pymysql://{}:{}@{}/{}'.format(db_user, db_pass, db_url, db_name))

conn = engine.connect()
# Write DataFrame into table_name
print('\nWrite table {} into db {}'.format(table_name, db_name))
df.to_sql(table_name, conn, if_exists='replace', index=False)

# Read data from table_name
df_read = pd.read_sql('select * from {}'.format(table_name), conn)
print('\nRead table {}'.format(df_read), df_read)

print('\n\n******** # 2nd part - Answer some questions using SQL ********')
# 1
df1 = pd.read_sql('select sum(value), ReportYear from {} where MeasureName = \'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' group by ReportYear'.format(table_name), conn)
print('\n1. Sum value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" per year\n', df1)

# 2
df2 = pd.read_sql('select ReportYear from {} where MeasureName = \'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' and ReportYear >= 2008 order by value desc limit 1'.format(table_name), conn)
print('\n2. Year with max value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" from year 2008 and later (inclusive)\n', df2)

# 3
df3 = pd.read_sql(
    'select max(value), MeasureName, StateName from {} group by MeasureName, StateName'.format(table_name), conn)
print('\n3. Max value of each measurement per state\n', df3)

# 4
df4 = pd.read_sql('select avg(value) avg_value, ReportYear, StateName from {} where MeasureName = \'Number of person-days with PM2.5 over the National Ambient Air Quality Standard (monitor and modeled data)\' group by ReportYear, StateName order by avg_value'.format(table_name), conn)
print('\n4. Average value of "Number of person-days with PM2.5 over the National Ambient Air Quality Standard (monitor and modeled data)" per year and state in ascending order\n', df4)

# 5
df5 = pd.read_sql('select StateName from (select max(value) max_value, StateName from {} where MeasureName = \'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' group by StateName order by max_value desc) as df5 limit 1'.format(table_name), conn)
print('\n5. State with the max accumulated value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" overall years\n', df5)

# 6
df6 = pd.read_sql('select avg(value) from {} where MeasureName = \'Number of person-days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' and StateName = \'Florida\''.format(table_name), conn)
print('\n6. Average value of "Number of person-days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" in the state of Florida\n', df6)

# 7
df7 = pd.read_sql('select tab.value, tab.CountyName, min_val_state_year.StateName, min_val_state_year.ReportYear from {} tab, (select min(value) min_value, StateName, ReportYear from {} where MeasureName = \'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' group by StateName, ReportYear) min_val_state_year where tab.MeasureName = \'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard\' and tab.value = min_val_state_year.min_value and tab.StateName = min_val_state_year.StateName and tab.ReportYear = min_val_state_year.ReportYear'.format(table_name, table_name), conn)
print('\n7. County with min "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" per state per year\n', df7)

conn.close()
