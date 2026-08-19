from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table='youtube_api'

@task
def staging_table():

    schema='staging'

    conn, cursorr = None, None

    try:
        conn, cursor = get_conn_cursor()

        yt_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)

        for row in yt_data:

            if len(table_ids) == 0:
                insert_rows(cursor, conn, schema, row)

            else:
                if row['video_id'] in table_ids:
                    update_rows(cursor, conn, schema, row)
                else:
                    insert_rows(cursor, conn, schema, row)

        ids_in_json = set([row['video_id'] for row in yt_data])

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cursor:
            close_conn_cursor(conn, cursor)

@task
def gold_table():

    schema='gold'

    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cursor, schema)

        current_video_ids = set()

        cursor.execute(f"SELECT * FROM staging.{table};")
        rows = cursor.fetchall()

        for row in rows:

            current_video_ids.add(row['video_id'])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cursor, conn, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row['video_id'] in table_ids:
                    update_rows(cursor, conn, schema, transformed_row)

                else:
                    insert_rows(cursor, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        # Ensure the connection and cursor are closed
        if conn and cursor:
            close_conn_cursor(conn, cursor)


