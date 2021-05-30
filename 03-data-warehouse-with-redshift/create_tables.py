import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('-- CREATING TABLES --')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('1 of 5 -- Connection Successful @{}:{}/{}'.format(config['CLUSTER']['HOST'], config['CLUSTER']['DB_PORT'], config['CLUSTER']['DB_NAME']))
    
    cur = conn.cursor()
    print('2 of 5 -- Cursor Created')

    drop_tables(cur, conn)
    print('3 of 5 -- Existing Tables Dropped')

    create_tables(cur, conn)
    print('4 of 5 -- Tables Created')

    conn.close()
    print('5 of 5 -- Connection Closed')
    print('-- TABLES COMPLETED --')

if __name__ == "__main__":
    main()