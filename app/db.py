import logging
import sqlite3

from app.config import config

conn_container = {}


def connect_db(connection_string):
    logging.debug(f"Connecting to: {connection_string} ...")
    conn_container["conn"] = sqlite3.connect(connection_string)
    conn_container["conn"].row_factory = sqlite3.Row


def get_connection():
    if "conn" in conn_container:
        return conn_container["conn"]
    else:
        connect_db(config["sql_connection_string"])
        return conn_container["conn"]


def sql_datatype(datatype, column_name):
    if datatype == "BOOLEAN":
        return f"BOOLEAN NOT NULL CHECK ({column_name} IN (0,1))"
    if datatype == "INTEGER":
        return "INTEGER"
    if datatype == "TEXT":
        return "TEXT"
    raise Exception(f"Unsupported data type encountered: {datatype}")


def create_table(data_file_format_type, schema):
    logging.info(f"Creating table (if not exists): {data_file_format_type}")
    columns = [
        f"{col_def['column name']} {sql_datatype(col_def['datatype'], col_def['column name'])}"
        for col_def in schema
    ]
    create_table_statement = f"""
        create table if not exists {data_file_format_type} 
            ({', '.join(columns)})"""

    conn = get_connection()
    with conn:
        conn.execute(create_table_statement)


def insert(table, rows):
    logging.info(f"Inserting {len(rows)} records into table: {table}")
    conn = get_connection()
    column_names = [row for row in rows[0].keys()]
    insert_statement = f"""
        insert into {table} 
            ({', '.join(column_names)}) 
            values ({', '.join([f':{row}' for row in column_names])})
        """
    with conn:
        conn.executemany(insert_statement, rows)
