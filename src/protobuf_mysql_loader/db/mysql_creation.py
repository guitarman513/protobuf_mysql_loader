from datetime import datetime, timezone
import mysql.connector
from dateutil.relativedelta import relativedelta

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mysql.connector import MySQLConnection

# view tables with a tool like MySQL Workbench
def create_initial_table(table_name:str, mysql_connection:"MySQLConnection") -> None:
    # NOTE: THIS TABLE NEEDS IMMEDIATE PARTITION CREATION!! ONLY HAS EMPTY BOUNDS RIGHT NOW.
    create_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    my_id BIG INT UNSIGNED NOT NULL AUTO_INCREMENT,
    some_time_utc TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) /* %Y-%m-%d %H:%M:%S.%f  or something like 2025-09-15 17:11:31.123456   */
    some_decimal_float DECIMAL(10,4) /* up to 10 sigfigs, up to four after the period like 123456.1234 */
    some_blob BLOB /* some binary data in bytes. Need to parse later  */
)

PARTITION BY RANGE (FLOOR(UNIX_TIMESTAMP(some_time_utc))) (
    PARTITION p_init VALUES LESS THAN (UNIX_TIMESTAMP('2024-01-01 00:00:00')),
    PARTITION p_max VALUES LESS THAN MAXVALUE
);
"""
    with mysql_connection.cursor() as cur:
        cur.execute(create_sql)
    mysql_connection.commit()
    return None

def _get_existing_partitions(mysql_connection, table_name) -> List[str]:
    query=f"""
SELECT PARTITION_NAME, PARTITION_DESCRIPTION /* name, desc something like (2025_01, 17234234234234) */
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_NAME= '{table_name}' AND PARTITION_NAME IS NOT NULL
ORDER BY PARTITION_DESCRIPTION;"""
    with mysql_connection.cursor() as cur:
        cur.execute(query)
        rows=cur.fetchall()
    
    # Drop p_max, will add in again later
    partition_names = [row[0] for row in rows] 
    if "pmax" in partition_names:
        with mysql_connection.cursor() as cur:
            cur.execute(f"ALTER TABLE {table_name} DROP PARTITION pmax")
            mysql_connection.commit()
    # return only numeric
    return [row[1] for row in rows if row[1] and row[1].isdigit()] # partition names like 2025_01 but this is description which is unixtimestamp

def _create_monthly_partitions(mysql_connection, table_name, months_back=2, months_forward=6):
    now = datetime.now().replace(day=1)
    monthyear_set = set()

    for offset in range(-months_back, months_forward+1):
        dt_baseline_not_zerod = now + relativedelta(months=offset)
        dt = datetime(dt_baseline_not_zerod.year, dt_baseline_not_zerod.month, 1, 0, 0, 0, tzinfo=timezone.utc)
        bound_time = dt + relativedelta(months=1)
        partition_name = f"p{dt.year}_{dt.month:02}"
        partition_bound = int(bound_time.timestamp())
        monthyear_set.add((partition_name, int(partition_bound), dt.year, dt.month))
    existing_bounds = set(int(b) for b in _get_existing_partitions(mysql_connection, table_name) if b.isdigit())

    partitions_to_add = [
        (name, bound, year, month) # something like (2025_01, 17223424523453, 1, 2025)
        for name, bound, year, month in monthyear_set
        if int(bound not in existing_bounds)
    ]

    if not partitions_to_add:
        print("No new partitions needed")
        return
    
    alter_sql = f"ALTER TABLE {table_name} ADD PARTITION ("
    alter_sql+= ",\n".join([f"PARTITION {name} VALUES LESS THAN ({bound})" for name, bound, _, _ in sorted(partitions_to_add, key=lambda x: x[1])])
    alter_sql+=');'

    with mysql_connection.cursor() as cur:
        cur.execute(alter_sql)
        mysql_connection.commit()
    
    with mysql_connection.cursor() as cur:
        cur.execute(f"ALTER TABLE {table_name} ADD PARTITION (PARTITION pmax VALUES LESS THAN (MAXVALUE));")
        mysql_connection.commit()

    print(f"Added {len(partitions_to_add)} new partition(s)")
    return


def add_partitions(table_name, mysql_conn, last_unix_timestamp_partitions_were_created):
    # only add partitions once a week or so
    if datetime.now().timestamp() - last_unix_timestamp_partitions_were_created > 7*24*60*60: # one week
        print("Checking if we should add new partitions, then adding them if necessary.")
        _create_monthly_partitions(mysql_conn, table_name)
    return
    



def add_tracks_to_db(records:List[MySQLRecord], mysql_connection, useful_global_state, table_name):
    execute_multiple_return_nothing(
        connection = mysql_connection
        query_without_values = f"INSERT INTO {table_name} ( {','.join([c for c in TABLE_COLUMNS])} ) VALUES ( {','.join(['%s']*len(TABLE_COLUMNS))} );",
        list_of_tuple_records = [(
            _object.attr1,
            _object.attr2,
            _object.attr3,
        ) for _object in records]
    )
    return