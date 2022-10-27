import sys
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import extras
from sqlalchemy import create_engine

from constants import *

conn_string = 'postgresql://{0}:{1}@{2}:5432/{3}'.format(DEV_DB_USER, DEV_DB_PASSWORD, DEV_DB_HOST, DEV_DB_NAME)
db = create_engine(conn_string)
conn = db.connect()

# Here you want to change your database, username & password according to your own values
param_dic = {
    "host": DEV_DB_HOST,
    "database": DEV_DB_NAME,
    "user": DEV_DB_USER,
    "password": DEV_DB_PASSWORD
}


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def prod_psql_to_dataframe(query, column_names, message):
    conn = None
    try:
        conn = psycopg2.connect(**param_dic)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=column_names)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
    print(datetime.now(), "Data fetched for {}".format(message))

    return df


def local_psql_to_dataframe(query, column_names):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = connect(param_dic)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=column_names)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print(datetime.now(), "Data fetched")
    return df


def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def item_details_to_db(df):
    df['averageQty'] = df['averageQty'].astype(int)
    df['itemId'] = df['itemId'].astype(int)
    df['thresholdQty'] = df['thresholdQty'].astype(int)
    try:
        for i, row in df.iterrows():
            query = """INSERT INTO precalculated_fraud_items(item_id, average_qty, threshold_qty, lambda_value, 
            prob_threshold_qty,start_date) VALUES({0},{1},{2},{3},{4},'{5}') ON CONFLICT(item_id) DO UPDATE SET 
            average_qty = {1}, threshold_qty = {2},lambda_value = {3},prob_threshold_qty = {4},start_date = '{5}';""" \
                .format(row.itemId, row.averageQty,
                        row.thresholdQty,
                        row.lambdaValue,
                        row.probThresholdQty,
                        row.startDate)
            db.execute(query)
        print("Values are updated and inserted into the pre-calculated table")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        conn.close()
        return 1
