import configparser
import psycopg2
import re
from sql_queries import copy_table_queries, insert_table_queries, validate_staging_queries, validate_insert_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def validate_staging(cur, conn):
    for query in validate_staging_queries:
        cur.execute(query)
        table = re.search('FROM (.*);',query)
        rows = cur.fetchone()
        print(rows[0], "rows staged to ", table.group(1))
        conn.commit()
        
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def validate_insert(cur, conn):
    for query in validate_insert_queries:
        cur.execute(query)
        table = re.search('FROM (.*);',query)
        rows = cur.fetchone()
        print(rows[0], "rows inserted to ", table.group(1))
        conn.commit()
        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('-- RUNNING ETL--')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('1 of 8 -- Connection Successful @{}:{}/{}'.format(config['CLUSTER']['HOST'], config['CLUSTER']['DB_PORT'], config['CLUSTER']['DB_NAME']))
    
    cur = conn.cursor()
    print('2 of 8 -- Cursor Created')

    print('3 of 8 -- Loading to Staging')
    load_staging_tables(cur, conn)
    print('4 of 8 -- Loaded to Staging')

    validate_staging(cur, conn)
    print('5 of 8 -- Staging Validated')
    
    insert_tables(cur, conn)
    print('6 of 8 -- Inserted into Tables')

    validate_insert(cur, conn)
    print('7 of 8 -- Insert Validated')

    conn.close()
    print('8 of 8 -- Connection Closed')
    print('-- ETL COMPLETED --')


if __name__ == "__main__":
    main()