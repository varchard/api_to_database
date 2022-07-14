import os
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

# vars
db_host = os.environ.get('db_host')
db_name = os.environ.get('db_name')
db_user = os.environ.get('db_user')
db_pass = os.environ.get('db_pass')

# get data to examine into df
conn = psycopg2.connect(dbname= db_name, user = db_user, password = db_pass, host = db_host) # sets connection to postgres database
cur = conn.cursor() # opens cursor in database
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}') # sets engine for select statement
df = pd.read_sql_query('''SELECT 
                    "index", "strIngredient1", "strIngredient2", "strIngredient3", "strIngredient4", "strIngredient5", "strIngredient6" 
                    FROM drinks''', engine)

# count number of ingredients in each cocktail
df['count_ingredients'] = df.apply(lambda x: x.count()-1, axis=1)
# print(df['count_ingredients'])

# insert number of ingredients into new column in postgres
df.to_sql(name= 'temp_table', con= engine, if_exists= 'replace')

# cur.execute('''ALTER TABLE drinks ADD number_ingredients INT;''')
cur.execute(f'''UPDATE drinks SET "number_ingredients"=temp_table."count_ingredients" from temp_table where drinks."index"=temp_table."index";''')

conn.commit()
cur.close() # close cursor
conn.close() # close database connection

