import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies all the data to each staging table using the queries in `copy_table_queries` list. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts relevant data from the staging tables to each Redshit table using the queries in `insert_table_queries` list. 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    - Loads the configuration file 
    
    - Establishes connection with AWS and gets cursor to it.  
    
    - Loads the previously created staging tables.  
    
    - Inserts relevant data into the redshit tables. 
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()