import psycopg
from dotenv import load_dotenv
from psycopg import Connection
from psycopg.rows import dict_row

from elt_sys import EltRunEnvironment

load_dotenv(verbose=True)
e = EltRunEnvironment()


def emit_console_event(event):
    print(f"ELTHUB Run ......: {event}")


def check_connection_to_elt_db():
    conn: Connection
    with psycopg.connect(e.pg_url, row_factory=dict_row) as conn:
        conn.autocommit = True
        cur = conn.execute("SELECT VERSION()")
        return cur.fetchone()


def main():
    emit_console_event("Starting up ELTHUB")
    emit_console_event(f"In Folder ..........: {e.fldr_inbox()}")
    emit_console_event(f"Out Folder .........: {e.fldr_outbox()}")
    emit_console_event(f"Server Version .....: {check_connection_to_elt_db()}")


if __name__ == "__main__":
    main()
