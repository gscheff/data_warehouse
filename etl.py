import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """redshift reads json files from s3 buckets into staging tables.

    Parameter
    ---------
    cur: db cursor
        A redshift cursor.
    conn: db connection
        A redshift connection.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Fill fact and dimension tables with data from staging tables.

    Parameter
    ---------
    cur: db cursor
        A redshift cursor.
    conn: db connection
        A redshift connection.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Data processing pipeline. Ingest json files into staging tables and from
    there fill fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = (
        psycopg2
        .connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()
                )
            )
        )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
