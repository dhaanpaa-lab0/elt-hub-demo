import logging
from os import path

import psycopg
from dotenv import load_dotenv
from psycopg import Connection
from psycopg.rows import dict_row
from sqlalchemy import create_engine

from elt_sys import EltRunEnvironment
import polars as pl
from polars import DataFrame

load_dotenv(verbose=True)
e = EltRunEnvironment()


def emit_console_event(event):
    logging.log(logging.INFO, event)


def check_connection_to_elt_db():
    conn: Connection
    with psycopg.connect(e.pg_url, row_factory=dict_row) as conn:
        conn.autocommit = True
        cur = conn.execute("SELECT VERSION()")
        return cur.fetchone()


def handle_notice(notice):
    logging.log(logging.INFO, notice.message_primary)


def check_process():
    conn: Connection
    with psycopg.connect(e.pg_url, row_factory=dict_row) as conn:
        conn.autocommit = True
        conn.add_notice_handler(handle_notice)

        cur = conn.execute("CALL simulate_long_running_process()")
        return None


def create_sqlalchemy_engine():
    return create_engine(e.pg_url_aa)


def normalize_filename_to_table(filename: str) -> str:
    base = path.basename(filename)
    name = path.splitext(base)[0]
    return f"stg_{name.lower().replace(' ', '_').replace('-', '_')}"


def test_csv_file(filename: str) -> tuple[DataFrame, str] | None:
    if not filename.lower().endswith(".csv"):
        return None

    df = pl.read_csv(filename)
    df = df.rename({col: col.lower().replace(" ", "_") for col in df.columns})

    if "fl_date" in df.columns:
        df = df.with_columns(
            pl.col("fl_date").str.strptime(pl.Date, format="%Y-%m-%d").alias("fl_date"),
            pl.col("fl_date").str.strptime(pl.Date).dt.year().alias("year"),
        )
    print(df.head())
    return df, normalize_filename_to_table(filename)


def main():
    logging.basicConfig(level=logging.INFO)
    emit_console_event("Starting up ELTHUB")
    check_process()

    emit_console_event(f"In Folder ..........: {e.fldr_inbox()}")
    emit_console_event(f"Out Folder .........: {e.fldr_outbox()}")
    emit_console_event(f"Server Version .....: {check_connection_to_elt_db()}")
    inbox_files = e.get_inbox_files_abs_path()
    if inbox_files:
        emit_console_event(f"Inbox Files ........: {len(inbox_files)}")
        for f in inbox_files:
            emit_console_event(f"Processing file: {f}")
            result = test_csv_file(f)
            if result is None:
                emit_console_event(f"Skipping file: {f}")
                continue
            df, table_name = result

            df.write_database(
                f"ingest.{table_name}",
                create_sqlalchemy_engine(),
                if_table_exists="replace",
            )
            emit_console_event(f"File processed: {f}")
    else:
        emit_console_event("No files found in inbox")


if __name__ == "__main__":
    main()
