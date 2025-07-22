from typing import List, Tuple

import mysql.connector
from mysql.connector import Error


def get_mysql_connection_object(
        host:str='localhost',
        user:str='root',
        password:str='password',
        database:str='some_database',
    ):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
    )


def check_on_mysql_connection(mysql_connection):
    try:
        # if connection is lost, ping will raise an error
        mysql_connection.ping(reconnect=True, attempts=5, delay=60)
    except Error as e:
        print("DB error:", e)
        try:
            mysql_connection = get_mysql_connection_object()
        except Error as reconnect_eror:
            print("Reconnect failed:", reconnect_eror)
    return None


def execute_single_sql_statement_returning_results(mysql_connection, sql_command:str):
    results = None
    try:
        cursor = mysql_connection.cursor()
        cursor.execute(sql_command)
        results = cursor.fetchall()
        mysql_connection.commit()
    except Exception as e:
        print(f"Error: {e}")
    return results


def execute_many_returning_nothing(mysql_connection, sql_query_with_placeholders:str, list_of_tuple_records:List[Tuple]):
    """
    Typical usage:
    sql_query_with_placeholders = f"INSERT INTO {table_name} ( {','.join([c for c in TABLE_COLUMNS])} ) VALUES ( {','.join(['%s']*len(TABLE_COLUMNS))} );"
    becomes something like "INSERT INTO my_table (col1, col2, col3) VALUES (%s, %s, %s);
    list_of_tuple_records = [ (val11, val12, val13), (val21, val22, val23), (val31, val32, val33) ]

    could probably make a more useful function here by having function parameters for TABLE_COLUMNS and doing this logic for them!
    
    """
    
    with mysql_connection.cursor() as cur:
        cur.executemany(sql_query_with_placeholders, list_of_tuple_records)
    mysql_connection.commit()
    return None