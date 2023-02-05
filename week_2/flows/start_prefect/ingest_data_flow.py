#!/usr/bin/env python
# coding: utf-8
import os
import csv
import time
import pathlib
import argparse
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

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


@task(log_prints=True, cache_key_fn=task_input_hash,
        cache_expiration=timedelta(days=1))
def extract_data(url: str, csv_name='output.csv'):

    BASE_DIR = pathlib.Path().resolve()
    DATASET_DIR = BASE_DIR / 'datasets'
    DATASET_DIR.mkdir(exist_ok=True, parents=True)
    csv_name = DATASET_DIR / 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    df = pd.read_csv(csv_name, compression='gzip')
    
    # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    return df

@task(log_prints=True)
def transform_data(df):
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(f"pre: missing passengers count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passengers count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True)
def login_db(user, password, host, port, db):
    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')



@task(log_prints=True, retries=3)
def load_data(engine, table_name, df):

    # engine = login_db(user, password, host, port, db)

    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # os.system(f"wget {url} -O {csv_name}")
    # df = pd.read_csv(csv_name, compression='gzip')
    # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])


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

@flow(name='Subflow')
def log_subflow(table_name: str):
    print(f'Name of the table is {table_name}')


@flow(name='ingest data')
def main_flow():
    user = 'postgres'
    password = '2130020'
    host = 'localhost'
    port = '5433'
    db = 'DataWarehouseX'
    table_name = 'green_taxi_data'
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    log_subflow(table_name)
    
    raw_data = extract_data(url)

    data = transform_data(raw_data)
    
    engine = login_db(user, password, host, port, db)
    load_data(engine, table_name, data)

    # conn_block = SqlAlchemyConnector.load("postgres-connector")
    # with conn_block.get_connection(begin=False) as engine:
    #     load_data(engine, table_name, data)

    

   


    


if __name__ == '__main__':
    # user, password, host, port, db name, table name
    # url of the csv
    # parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # parser.add_argument('--user', help='username for postgres')
    # parser.add_argument('--password', help='password for postgres')
    # parser.add_argument('--host', help='host for postgres')
    # parser.add_argument('--port', help='port for postgres')
    # parser.add_argument('--db', help='database name for postgres')
    # parser.add_argument('--table_name', help='name of thentable where result is written to')
    # parser.add_argument('--url', help='url of the csv')

    # args = parser.parse_args()

    main_flow()