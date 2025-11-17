from __future__ import annotations

import duckdb
from pathlib import Path


THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
DATA_DIR = PROJECT_ROOT / "data"
LOCALDB_DIR = DATA_DIR / "localdb"

DB_NAME = "wow_data.db"


def get_duckdb_path() -> Path:
    """
    Devuelve la ruta absoluta al archivo DuckDB.
    """
    LOCALDB_DIR.mkdir(parents=True, exist_ok=True)
    return LOCALDB_DIR / DB_NAME


def get_connection():
    """
    Devuelve una conexión a DuckDB.
    Si el archivo no existe, DuckDB lo crea automáticamente.
    """
    db_path = get_duckdb_path()
    conn = duckdb.connect(str(db_path))
    return conn


def run_sql(conn, query: str):
    """
    Ejecuta SQL simple (CREATE TABLE, INSERT, etc).
    """
    conn.execute(query)