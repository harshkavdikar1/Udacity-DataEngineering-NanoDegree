import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the data to staging tables from s3 using list 
    copy_table_queries
    Args:
        cur: Cursor object to the database
        conn: Connection object to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load the data to target tables from staging tables using 
    list insert_table_queries
    Args:
        cur: Cursor object to the database
        conn: Connection object to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads the config file to get connection configurations.

    Establishes connection to the database.

    Load the data to staging and target tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
