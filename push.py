import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from ddlgenerator.ddlgenerator import Table
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import re
import time
import config
import data_dispenser
import logging

logger=logging.getLogger(__name__)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

fh = logging.FileHandler('logs/push.log')
fh.setLevel(logging.ERROR)
fh.setFormatter(formatter)

logger.addHandler(fh)


def create_schema(schema_name: str, host: str, port: int, database: str, user: str, password: str):
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE;')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
        conn.commit()

    conn.close()


def _open_csv(filename):
    """Opens a file in binary mode if its name ends with 'pickle'"""
    if filename.lower().endswith('.pickle'):
        file_mode = 'rb'
    elif filename.lower().endswith('.csv'):
        file_mode = 'r'
    else:
        file_mode = 'rU'
    input_source = open(filename, file_mode)
    return input_source


def create_sql(file_name: str, table_name: str):
    table = Table(data=file_name, table_name=table_name)
    sql = table.sql(dialect='postgresql', inserts=True)

    return sql


def push_one_csv_to_one_table(file_name: str, idx: int, total_size: int, conn_pool):
    try:
        print(f'Processing {idx + 1}/{total_size}: \t{file_name}')

        table_name = file_name[file_name.rindex('/') + 1:file_name.rindex('.')]
        sql = create_sql(file_name=file_name, table_name=table_name)

        conn = conn_pool.getconn()
        
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

        print(f'Pushed the table {idx + 1}/{total_size}: \t{re.sub(r"[-.]+", "_", table_name).lower()}')

    except Exception as ex:
        logger.error(f'While processing {file_name}, an exception occurs: {ex}')
    finally:
        conn_pool.putconn(conn)


def push_all_csv(conn_pool, n_threads: int):
    file_names = sorted(glob.glob('data/prepared/*.csv', recursive=True))
    total_size = len(file_names)

    # flavour 1: for debug purpose
    # for idx, file_name in enumerate(file_names):
    #     push_one_csv_to_one_table(file_name=file_name, idx=idx, total_size=total_size, conn_pool=conn_pool)

    # flavour 2: for multi-thread execution
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(push_one_csv_to_one_table, file_name, idx, total_size, conn_pool) for idx, file_name in enumerate(file_names)]

        for future in as_completed(futures):
            print(future)


def connect_and_push(push_function, schema_name: str, maxconn: int, n_threads: int, host: str, port: int, database: str, user: str, password: str):

    pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=maxconn,
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        options=f'-c search_path={schema_name}'
    )

    push_function(conn_pool=pool, n_threads=n_threads)


if __name__ == "__main__":
    # There is a bug in "_open" function in the "data_dispenser" module.
    # Use "open_csv" function to override the original "_open" function.
    data_dispenser.sources._open = _open_csv

    # time the execution
    start_time = time.monotonic()

    # load configs
    config_map = config.load('config/config.yml')
    print(config_map)

    schema_name = config_map['push']['schema_name']
    maxconn = config_map['push']['maxconn']
    n_threads = config_map['push']['n_threads']
    host = config_map['push']['host']
    port = config_map['push']['port']
    database = config_map['push']['database']
    user = config_map['push']['user']
    password = config_map['push']['password']

    # create the schema
    create_schema(schema_name=schema_name,
                  host=host,
                  port=port,
                  database=database,
                  user=user,
                  password=password)

    # connect to the database and push the data
    connect_and_push(push_function=push_all_csv,
                     schema_name=schema_name,
                     maxconn=maxconn,
                     n_threads=n_threads,
                     host=host,
                     port=port,
                     database=database,
                     user=user,
                     password=password)

    end_time = time.monotonic()
    print(f'Total execution time: {int((end_time - start_time)/60)} min')
