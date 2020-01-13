#!/usr/bin/env python
import os
import sys
import yaml
import requests
import pyodbc



# Find working path:
def WorkingPath():
    # Script path, else current working dir:
    if '__file__' in globals():
        return os.path.dirname(os.path.realpath(__file__))
    else:
        return os.getcwd()


# Load yaml file:
def LoadYaml(file_name: str):
    try:
        return yaml.load(open(os.path.join(base_path, file_name), 'r'), Loader=yaml.FullLoader)
    except yaml.YAMLError as error:
        print(error)
        sys.exit(1)

# Determine paths:
base_path = WorkingPath()
temp_path = os.path.join(base_path, 'Temp')
if not os.path.exists(temp_path):
    os.makedirs(temp_path)

# Load config files:
config = LoadYaml('config_CTO_Scopie.yaml')
auth = LoadYaml('auth_CTO_Scopie.yaml')
TableStruc = LoadYaml ('TableStruc_CTO.yaml')

# Determine config:
base_url = config['Api']['BaseUrl']
print('Using base URL {}'.format(base_url))

# Execute a prepared sql statement
def ExecSql(sql):
    try:
        cursor.execute(sql)
        cursor.commit()
    except pyodbc.Error as error:
        sqlstate = error.args[1]
        print(sqlstate)

# Prepare global database connection:
sql_server = config['Sql']['Server']
sql_database = config['Sql']['Database']
sql_schema = config['Sql']['Schema']

for endpoint in auth['Api']:
    dashboard_name = endpoint['Naam']
    dashboard_tablestruc = TableStruc['Table_structure'][dashboard_name]

# Start HTTP session:
    ses = requests.Session()
    full_url = '?key='.join([base_url, endpoint['Token']])
    print('Using full URL {}'.format(full_url))

    # Perform login:
    try:
        response = ses.post(full_url, data={'password': endpoint['Wachtwoord']})
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_error:
        print("HTTP Error:", http_error)
        sys.exit(1)
    except requests.exceptions.RequestException as error:
        print("Error", error)
        sys.exit(1)
    print('HTTP {}'.format(response.status_code))

    file_path = os.path.join(temp_path, ''.join(['CTO_', endpoint['BestandNaam'], '.csv']))
    open(file_path, 'wb').write(response.content)

#YAML to SQL database
    sql_table = config['Sql']['Table']
    print('Connecting to database {} on server {} as login {}\\{}.'.format(
            sql_server, sql_database, os.environ['userdomain'], os.getlogin()))
    sql_connection = (
        'DRIVER={{ODBC Driver 17 for SQL Server}};'
        'SERVER={};'
        'DATABASE={};'
        'Trusted_Connection=Yes;'
        'APP=Python - Load_CTO_Scopie;'
        ).format(sql_server, sql_database)
    cnxn = pyodbc.connect(sql_connection)
    cursor = cnxn.cursor()
    preptable_sql = (
            "DROP TABLE IF EXISTS {0}.{1}; "
            "CREATE TABLE {0}.{1} ("
            "{2}"
            ");"
                       ).format(sql_schema, sql_table)
    ExecSql(preptable_sql)

#Insert data into specific SQL tabel
    bulkload_sql = (
           "BULK INSERT {}.{} "
           "FROM '{}' "
           "WITH ("
               "FIRSTROW = 2"
               ", FIELDTERMINATOR = '\"\,\"\'"
               ", ROWTERMINATOR = '\"\n\0X0A\'" # LF character naar de volgende rij en verwijderen " in laatste kolom
               ", TABLOCK"
               ", MAXERRORS = 0"
                 ");"
           ).format(sql_schema, sql_table, file_path)
    ExecSql(bulkload_sql)
#sql_table = TableStruc['Table_structure']['Table Scopie']
#print('Database {} on schema {} is created {}\\{} .'.format(sql_table))
