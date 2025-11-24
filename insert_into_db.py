import re 
import os 
import json
import pandas as pd 
from dotenv import load_dotenv
from db_config import PostgreSQL


load_dotenv()


with open("task1_d.json", "r") as json_file:
    invalid_content = json_file.read()


valid_format = re.sub(r':(\w+)=>', r'"\1":', invalid_content)


def prepare_to_insert():
    values = []
    for row in json.loads(valid_format):
        values.append(
            (
                row['id'],
                row['title'],
                row['author'],
                row['genre'],
                row['publisher'],
                row['year'],
                row['price'][1:],
                "USD" if row['price'].startswith("$") else "EUR"
            )
        )
    return values


db = PostgreSQL(
    db_name=os.getenv('DB_NAME'), 
    db_user=os.getenv('DB_USER'), 
    db_password=os.getenv('DB_PASSWORD')
    )


if db.connect():
    """
        I added additional field to books table to check later on and transform from euro into usd
    """
    db.execute_query("""
        CREATE TABLE IF NOT EXISTS books (
            id NUMERIC(25) PRIMARY KEY,                 
            title TEXT,                 
            author TEXT,               
            genre TEXT,                
            publisher TEXT,            
            year INTEGER,             
            price DECIMAL(10,2),
            currency VARCHAR(3) 
        )
    """)
    row_count = db.execute_query(
        """SELECT count(*) FROM books""", 
        select_query=True
    )
    if row_count == 0:
        db.execute_query(
            """INSERT INTO books VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""", 
            many_query=True, 
            values=prepare_to_insert()
        )
    db.close_conn()


