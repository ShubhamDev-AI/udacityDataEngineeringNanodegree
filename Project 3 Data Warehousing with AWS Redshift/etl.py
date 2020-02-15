import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Perform a bulk load of JSON files located in AWS S3 Buckets.
    """
    for query in copy_table_queries:
        print('executing query: ',query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Move data from staging tables to their target Dimensional tables.
    Transform data during this process to fit Dimensional requirements.
    """
    for query in insert_table_queries:
        print('executing query: ',query)
        cur.execute(query)
        conn.commit()


def main():
    """
        Read connection credentials from config file and then call functions
    to perform and Extract, Load and Transform process.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host=config.get('cluster_settings','endpoint')
    dbname=config.get('cluster_settings','db_name')
    user=config.get('cluster_settings','master_user_name')
    password=config.get('cluster_settings','master_user_password')
    port=int(config.get('cluster_settings','port'))

    conn = psycopg2.connect(f"""
        host={host}
        dbname={dbname}
        user={user}
        password={password}
        port={port}
    """)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()