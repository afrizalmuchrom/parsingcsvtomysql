import pymysql
import pandas as pd
import sys
import os


def csv_to_mysql(load_sql, host, user, password, db):
    '''
    This function load a csv file to MySQL table according to
    the load_sql statement.
    '''
    try:
        con = pymysql.connect(host=host,
                                user=user,
                                password=password,
                                database=db,
                                autocommit=True,
                                local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(load_sql)
        print('Succuessfully loaded the table from csv.')
        con.close()
       
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)

# Execution Example

host = '153.92.5.166'
user = 'data_logger'
db = 'data_logger'
password = 'data_logger2018'

directory = './pama'
for filename in os.listdir(directory):
    if filename.endswith(".txt"): 
         csv_to_mysql("""LOAD DATA LOCAL INFILE 'D:/\/belajar/\python/\pama\/""" + str(filename) + """' INTO TABLE datalog;""", host, user, password, db)
         print(filename)
        # continue
    else:
        continue