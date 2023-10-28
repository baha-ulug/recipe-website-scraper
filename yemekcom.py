import json
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import requests
import psycopg2

load_dotenv()
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DATABASE = os.getenv("DB_DATABASE")
SCHEMA = os.getenv("DB_SCHEMA")
TABLE = os.environ.get("DB_TABLE")
formatted_date = datetime.now().strftime("%Y.%m.%d %H:%M:%S")

def get_urls():
    start_urls = []
    response = requests.get('https://zagorapi.yemek.com/search/recipe?Start=0&Rows=12')
    
    # Check if the request was successful
    if response.status_code == 200:
        # Extract the data from the response object
        data = response.json()
        total_recipe_count = data["Total"]
        print("total recipe count is: ",total_recipe_count)
    else:
        print("Error: Request failed with status code", response.status_code)
    total_recipe_count = 120
    for i in range(0, total_recipe_count, 12):
        start_urls.append(f'https://zagorapi.yemek.com/search/recipe?Start={i}&Rows=12')
    return start_urls

def get_df(start_urls): 
    df = pd.DataFrame(columns=['scrape_date', 'start_url', 'recipe_json'])
    for url in start_urls:
        print("url for request is:", url)
        response = requests.get(url)
        recipes = response.json()
        for recipe in recipes["Data"]["Posts"]:
            df = pd.concat([df, pd.DataFrame([{
        'scrape_date': formatted_date,
        'start_url': url,
        'recipe_json': json.dumps(recipe, ensure_ascii=False)
    }])])
    
    return df

def db_insert(df):
    conn = None
    # Connect to the database
    conn = psycopg2.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
    print("connection is created")

    # Create a cursor object
    cur = conn.cursor()
    print("cursor is created")

    # set the encoding of the database connection to UTF-8
    cur.execute("SET client_encoding = 'UTF8'")
    
    # Create schema if not exists
    cur.execute(f'''CREATE SCHEMA IF NOT EXISTS {SCHEMA}''')

    # Create table if not exists
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
        id serial4 NOT NULL, 
        scrape_date timestamp,
        page_url text,
        recipe_json jsonb,
        CONSTRAINT {TABLE}_pkey PRIMARY KEY (id))""")

    sql_query = f"""INSERT INTO {SCHEMA}.{TABLE} (scrape_date, page_url, recipe_json) VALUES (%s, %s, %s)"""
    batch_size = 1000

    # Insert values in batches
    for i in range(0, len(df), batch_size):
        batch_df = df[i:i+batch_size]
        batch_values = batch_df.to_records(index=False).tolist()
        cur.executemany(sql_query, batch_values)
        # Commit the changes to the database
        conn.commit()
    print("changes are commited")

    # Close the cursor and connection
    cur.close()
    conn.close()
    print("cursor and connection are closed")

def main():    
    start_urls = get_urls()
    df = get_df(start_urls)
    db_insert(df)
    return "Success!"

if __name__=='__main__':
    main()