import requests 
import pandas as pd 
import datetime
import psycopg2 
from sqlalchemy import create_engine
import os
from re import compile

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
df.drop(columns = ['strVideo','strIBA','strInstructionsES', 'strInstructionsDE', 'strInstructionsIT', 
    'strDrinkThumb','strImageSource', 'strImageAttribution'], inplace = True)
df['timestamp'] = datetime.datetime.now() # appends timestamp of when data was collected

def unit_scrub(value):
    '''accepts value from column with multiple units of measure and returns measure in Oz'''
    num_search = compile('\d*\.\d*|\d')
    cl_search = compile('cl')
    part_search = compile('part')
    shot_search = compile('shot|Shot')
    try:
        value = value.replace('1/8', '.125')
        value = value.replace('1/4', '.25')
        value = value.replace(' 1/2', '.5')
        value = value.replace('1/2','.5')
        value = value.replace(' 3/4', '.75')
        value = value.replace('1/3', '.333')
    except AttributeError:
        return None
    if cl_search.search(value):
        num = round((float((num_search.findall(value))[0]) * 0.33814),2)
        cleaned = str(num) + ' oz'
        return cleaned
    elif part_search.search(value):
        num = round((float(num_search.findall(value)[0]) / 2), 2)
        cleaned = str(num) + ' oz'
        return cleaned
    elif shot_search.search(value):
        num = round((float(num_search.findall(value)[0]) * 1.5), 2)
        cleaned = str(num) + ' oz'
        return cleaned
    else:
        return value

cols_to_clean = ['strMeasure1', 'strMeasure2', 'strMeasure3', 'strMeasure4', 'strMeasure5', 'strMeasure6']
for col in cols_to_clean:
    df[col] = df[col].map(unit_scrub)

# df.to_clipboard()

#send to postgres server
conn = psycopg2.connect(dbname= db_name, user = db_user, password = db_pass, host = db_host) # sets connection to postgres database
cur = conn.cursor() # opens cursor in database
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}') # sets engine for to_sql statement
df.to_sql(name='drinks',con= engine, if_exists = 'replace') # exports dataframe to database
conn.commit() # runs commit statement in database
cur.close() # close cursor
conn.close() # close database connection