from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import List

import pandas as pd


THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Reutilizamos la conexión local
from tp2025.io.load_localdb import get_connection, run_sql

DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"

TABLE_NAME = "wow_data.main.raw_chinfo"


def get_processing_date_str() -> str:
    """
    Devuelve la fecha de proceso en formato YYYYMMDD.
    Debe matchear el nombre del parquet generado por extract_chinfo_to_landing.
    """
    return date.today().strftime("%Y%m%d")


def list_parquet_for_processing_date(processing_date: str) -> List[Path]:
    """
    Lista los parquet de character info para la fecha dada.
    Un único archivo del estilo:
      ch_profile_{processing_date}.parquet
    """
    pattern = f"ch_profile_{processing_date}.parquet"
    return list(LANDING_DIR.glob(pattern))


def load_parquet_to_dataframe(path: Path) -> pd.DataFrame:
    """
    Lee el parquet y lo normaliza a las columnas esperadas en RAW.
    Todo se guarda como string (igual que el leaderboard RAW).
    """
    df = pd.read_parquet(path)

    expected_cols = [
        "id",
        "name",
        "realm_slug",
        "faction",
        "class",
        "spec",
        "a_ilvl",
        "e_ilvl",
        "fecha_proceso",
    ]

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas esperadas en {path}: {missing}")

    # Reordenamos y forzamos a string para RAW
    df = df[expected_cols]
    df = df.astype(str)

    return df


def ensure_table_exists() -> None:
    """
    Crea el esquema y la tabla RAW si no existen.
    """
    conn = get_connection()

    run_sql(conn, "CREATE SCHEMA IF NOT EXISTS wow_data;")
    run_sql(conn, "CREATE SCHEMA IF NOT EXISTS wow_data.main;")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id            VARCHAR,
        name          VARCHAR,
        realm_slug    VARCHAR,
        faction       VARCHAR,
        class         VARCHAR,
        spec          VARCHAR,
        a_ilvl        VARCHAR,
        e_ilvl        VARCHAR,
        fecha_proceso VARCHAR
    );
    """
    run_sql(conn, ddl)


def load_into_duckdb(processing_date: str) -> None:
    """
    Lee el/los parquet de character profile de landing y los inserta en RAW.
    """
    files = list_parquet_for_processing_date(processing_date)
    if not files:
        print(
            f"[load_chinfo_raw_to_db] No hay parquet ch_profile_ "
            f"para processing_date={processing_date}"
        )
        return

    dfs = [load_parquet_to_dataframe(p) for p in files]
    full_df = pd.concat(dfs, ignore_index=True)

    ensure_table_exists()

    conn = get_connection()

    # Insertamos con DuckDB usando una tabla temporal en memoria
    conn.register("tmp_chinfo_df", full_df)
    conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM tmp_chinfo_df;")
    conn.unregister("tmp_chinfo_df")

    print(
        f"[load_chinfo_raw_to_db] Cargadas {len(full_df)} filas "
        f"en tabla {TABLE_NAME}"
    )


def run_load_chinfo_raw_to_db() -> None:
    processing_date = get_processing_date_str()
    load_into_duckdb(processing_date)


if __name__ == "__main__":
    run_load_chinfo_raw_to_db()