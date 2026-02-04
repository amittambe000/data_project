from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import RealDictCursor
from sqlalchemy_utils.functions import database

table = "yt_api"


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor


def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()


def create_schema(schema_name):
    conn, cur = get_conn_cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.commit()
    close_conn_cursor(conn, cur)


def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == "staging":
        table_sql = f""" CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_title" TEXT NOT NULL,
            "Upload_date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        )
        """
    else:
        table_sql = f""" CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_id" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_title" TEXT NOT NULL,
            "Upload_date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Type" VARCHAR(10) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comments_Count" INT
        )
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    cur.execute(f'SELECT "Video_id" FROM {schema}.{table}')
    ids = cur.fetchall()
    video_ids = [row["Video_id"] for row in ids]
    return video_ids
