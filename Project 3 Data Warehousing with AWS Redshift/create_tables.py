import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries, drop_schema_queries


def drop_schemas(cur, conn):
    for query in drop_schema_queries:
        print('executing query:',query)
        cur.execute(query)
        conn.commit()

def create_schemas(cur, conn):
    for query in create_schema_queries:
        print('executing query:',query)
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print('executing query:',query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print('executing query:',query)
        cur.execute(query)
        conn.commit()

def main():
    """
        Read database connection credentials from config file and then call
    functions to drop and recreate database schemas and tables.
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

    drop_schemas(cur, conn)
    create_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()