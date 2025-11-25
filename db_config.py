import os 
import time
import pandas as pd
import psycopg2


class PostgreSQL():
    def __init__(self, db_name, db_user, db_password):
        self.connection_params = {
            'host': 'localhost',        
            'database': db_name,
            'user': db_user,
            'password': db_password,
            'port': 5432                
        }
        self.conn = None 

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            print("Connection successful")
            return True
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            return False

    def execute_query(self, query, many_query=False, values=None, select_rowc_count_query=False, dataframe_query=False):
        if self.conn is not None:
            try: 
                cursor = self.conn.cursor()
                if many_query:
                    cursor.executemany(query, values)
                elif select_rowc_count_query:
                    cursor.execute(query)
                    row_count = cursor.fetchone()[0]
                    cursor.close()
                    return row_count
                elif dataframe_query:
                    df = pd.read_sql_query(query, self.conn)
                    df.index += 1
                    cursor.close()
                    return df 
                else:    
                    cursor.execute(query)
                self.conn.commit()
                cursor.close()
                print("Query executed")
            except psycopg2.Error as e:
                print(f"Query execution failed: {e}")

    def close_conn(self):
        if self.conn is not None:
            self.conn.close()
            print("Connection closed")