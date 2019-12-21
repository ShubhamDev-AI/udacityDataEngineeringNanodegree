import psycopg2
import os
from sql_queries import create_table_queries, drop_table_queries

user = os.environ["POSTGRES_USER"] # UDACITY ORIGINAL: student
password = os.environ["POSTGRES_PASSWORD"] # UDACITY ORIGINAL: student
host = '127.0.0.1' # UDACITY ORIGINAL: 127.0.0.1
dbname = 'postgres' # UDACITY ORIGINAL: studentdb

def create_database():
    # connect to default database
    conn = psycopg2.connect(
         host=host
        ,dbname=dbname
        ,user=user
        ,password=password
    )

    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(
         host=host
        ,dbname='sparkifydb'
        ,user=user
        ,password=password
    )
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()