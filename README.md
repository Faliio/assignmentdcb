# assignmentdcb
Assignment dcb


## Tech/framework used

<b>Built with</b>
- Python 3.8.2

<b>Python libraries</b>
- requests
- json
- pandas
- sqlite3
- urllib

## Usage
dataset_to_sql.py '<URL_JSON>' '<table_name>' '<connection_db_URL>'

The script src / dataset_to_sql.py has as input:
- URL_JSON -> URL of the JSON dataset
- table_name -> Name of the target table
- connection_db_URL -> DB connection URL

## Features
### Ingest data into a relational database from JSON files

The script reads a JSON-formatted dataset from the URL and inserts the data into the database specified by connection_db_URL in the table_name table. SQLite is used in this example.

### Answer some questions using SQL
In the second part the script executes some queries:

1.
```
    select sum(value), ReportYear 
	from table_name 
	where MeasureName = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
	group by ReportYear
```
2.
```
    select ReportYear 
	from table_name 
	where MeasureName = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
	and ReportYear >= 2008 
	order by value desc 
	limit 1
```
3.
```
    select max(value), MeasureName, StateName 
	from table_name 
	group by MeasureName, StateName
```
4.
```
    select avg(value) avg_value, ReportYear, StateName 
	from table_name 
	where MeasureName = 'Number of person-days with PM2.5 over the National Ambient Air Quality Standard (monitor and modeled data)' 
	group by ReportYear, StateName 
	order by avg_value
```
5.
```
    select StateName 
	from (select max(value) max_value, StateName 
		from table_name 
		where MeasureName = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
		group by StateName 
		order by max_value desc) 
	limit 1
```
6.
```
    select avg(value) 
	from table_name 
	where MeasureName = 'Number of person-days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
	and StateName = 'Florida'
```
7.
```
    select tab.value, tab.CountyName, min_val_state_year.StateName, min_val_state_year.ReportYear 
	from table_name tab, (select min(value) min_value, StateName, ReportYear 
		from table_name 
		where MeasureName = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
		group by StateName, ReportYear) min_val_state_year 
	where tab.MeasureName = 'Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard' 
	and tab.value = min_val_state_year.min_value 
	and tab.StateName = min_val_state_year.StateName 
	and tab.ReportYear = min_val_state_year.ReportYear'
```
### Design
A diagram explaining a solution of the design exercise (<a href="https://github.com/Faliio/assignmentdcb/raw/main/resources/3rdPart-Design.pdf">PDF Version</a>).
<img src="https://github.com/Faliio/assignmentdcb/blob/main/resources/3rdPard-Design.svg?raw=true"/>