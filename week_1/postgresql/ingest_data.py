#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
import csv
import time
from io import StringIO
from sqlalchemy import create_engine

# Custom insert method for DataFrame.to_sql
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        table_name = f'"{table.schema}"."{table.name}"'
        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    df = pd.read_csv(csv_name, compression='gzip')
    
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    start_time = time.time()
    with engine.begin() as conn:
        df.to_sql(
            table_name, #"green_taxi_data",
            conn,
            schema="public",
            index=False,
            method=psql_insert_copy,
            if_exists="replace")
    print(f'Fast custom insert required {time.time() - start_time:.1f} seconds.')
    
    # df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    # df.to_sql(name=table_name, con=engine, if_exists='replace')

    print('Data has successfully loaded')

    # df_iter = pd.read_csv('green_tripdata_2019-01.csv.gz', iterator=True, chunksize=100000)
    # pd.read_sql('select count(*) from green_taxi_data', con=engine)
    # while True:
    #     start = time()
    #     df = next(df_iter)
    #     df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
    #     end = time()
    #     print(f'inserted another chunck..., took {round((end-start),3)} seconds')


if __name__ == '__main__':
    # user, password, host, port, db name, table name
    # url of the csv
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of thentable where result is written to')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()

    main(args)