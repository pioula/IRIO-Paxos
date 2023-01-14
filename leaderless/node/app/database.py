import psycopg2
import os

from config import config

def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # read connection parameters
        params = config()

        if "NODE_ID" in os.environ:
          params["database"] += os.environ["NODE_ID"]
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        return psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def read_query(cur, query, params = ()):
  cur.execute(query, params)
  return cur.fetchall()

def write_query(cur, query, params = ()):
  cur.execute(query, params)