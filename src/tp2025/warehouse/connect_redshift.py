from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection as PgConnection, cursor as PgCursor

load_dotenv()


def get_redshift_uri() -> str:
    uri = os.getenv("REDSHIFT_URI")
    if not uri:
        raise RuntimeError("Falta la variable de entorno REDSHIFT_URI para conectarse a Redshift.")
    return uri


def get_default_schema() -> str | None:
    schema = os.getenv("REDSHIFT_SCHEMA")
    return schema.strip('"') if schema else None


def get_connection() -> PgConnection:
    conn = psycopg2.connect(get_redshift_uri())

    schema = get_default_schema()
    if schema:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
        conn.commit()

    return conn


@contextmanager
def redshift_cursor() -> Generator[PgCursor, None, None]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield cur
            conn.commit()
    finally:
        conn.close()
