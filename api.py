import requests 
import pandas as pd 
import datetime
import psycopg2 
from sqlalchemy import create_engine
import os

# vars
db_host = os.environ.get('db_host')
db_name = os.environ.get('db_name')
db_user = os.environ.get('db_user')
db_pass = os.environ.get('db_pass')
url = 'http://www.thecocktaildb.com/api/json/v1/1/search.php?f=a'  # URL API of cocktails by first name
dataset = []  # empty list to store API response

# query
r = requests.get(url)  # get function references API URL
if r.status_code != 200:
    raise ValueError("API returned bad status code")
dataset = r.json().get('drinks') # stores the API response
df = pd.DataFrame(dataset) #converts dataset to pandas dataframe

# clean
df.dropna(axis=1, how='all', inplace=True) # drops unused columns
df['timestamp'] = datetime.datetime.now() # appends timestamp of when data was collected

#send to postgres server
conn = psycopg2.connect(dbname= db_name, user = db_user, password = db_pass, host = db_host) # sets connection to postgres database
cur = conn.cursor() # opens cursor in database
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}') # sets engine for to_sql statement
df.to_sql(name='drinks',con= engine, if_exists = 'replace') # exports dataframe to database
conn.commit() # runs commit statement in database
cur.close() # close cursor
conn.close() # close database connection