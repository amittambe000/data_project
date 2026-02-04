import logging


from airflow.decorators import task

from datawarehouse.data_modification import delete_rows, insert_rows, update_rows
from datawarehouse.data_transformation import transform_data
from datawarehouse.data_utils import (
    close_conn_cursor,
    create_schema,
    create_table,
    get_conn_cursor,
    get_video_ids,
)
from datawarehouse.data_loading import load_data


logger = logging.getLogger(__name__)
table = "yt_api"


@task
def staging_table():
    try:
        schema = "staging"
        conn, curr = get_conn_cursor()
        YT_data = load_data()
        create_schema(schema)
        create_table(schema)
        table_ids = get_video_ids(curr, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(curr, conn, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(curr, conn, schema, row)
                else:
                    insert_rows(curr, conn, schema, row)
        ids_in_json = {row["video_id"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json
        if ids_to_delete:
            delete_rows(curr, conn, schema, ids_to_delete)
            logger.info(f"Deleted {len(ids_to_delete)} rows from {schema}")
    except Exception as e:
        logger.error(f"Error staging table: {e}")
        raise e
    finally:
        if conn and curr:
            close_conn_cursor(conn, curr)


@task
def core_table():

    schema = "core"

    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row["Video_id"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row["Video_id"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)

                else:
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        # Ensure the connection and cursor are closed
        if conn and cur:
            close_conn_cursor(conn, cur)
