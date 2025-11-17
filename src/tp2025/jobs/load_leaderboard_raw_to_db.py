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

from tp2025.io.load_localdb import get_connection, run_sql


LANDING_DIR = PROJECT_ROOT / "data" / "landing"

TABLE_NAME = "raw_pvp_leaderboard"


def list_parquet_for_processing_date(processing_date: str) -> List[Path]:
    """
    Lista solo los archivos parquet de la corrida actual.
    Formato esperado:
      pvp_leaderboard_s{season_id}_{bracket}_{processing_date}.parquet
    """
    if not LANDING_DIR.exists():
        raise FileNotFoundError(f"No existe landing: {LANDING_DIR}")

    files = []
    for p in LANDING_DIR.iterdir():
        if not p.is_file() or not p.name.endswith(".parquet"):
            continue
        if not p.name.startswith("pvp_leaderboard_s"):
            continue
        if processing_date in p.name:
            files.append(p)

    return sorted(files)


def parse_metadata(path: Path):
    """
    De:
        pvp_leaderboard_{season_id}_{bracket}_{processing_date}.parquet
    extrae:
        s_id = {season_id}
        bracket = {bracket}
        fecha_proceso = {processing_date}
    """
    stem = path.stem
    parts = stem.split("_")

    season_raw = parts[2]
    bracket = parts[3]
    fecha = parts[4]

    s_id = int(season_raw.lstrip("s"))

    return s_id, bracket, fecha


def load_parquet_to_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)

    s_id, bracket, fecha = parse_metadata(path)

    df["s_id"] = str(s_id)
    df["bracket"] = bracket
    df["fecha_proceso"] = fecha

    # Convertir todo a string (texto crudo en RAW)
    df = df.astype(str)

    # Reordenar columnas
    ordered = [
        "id", "name", "slug", "faction",
        "rank", "rating", "played", "won", "lost",
        "bracket", "s_id", "fecha_proceso"
    ]

    # en caso de columnas no esperadas:
    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]


def ensure_table_exists(conn):
    """
    Crea la tabla RAW si no existe.
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id TEXT,
        name TEXT,
        slug TEXT,
        faction TEXT,
        rank TEXT,
        rating TEXT,
        played TEXT,
        won TEXT,
        lost TEXT,
        bracket TEXT,
        s_id TEXT,
        fecha_proceso TEXT
    );
    """
    run_sql(conn, query)


def load_into_duckdb(processing_date: str):
    """
    Lee los parquet de la fecha de procesamiento y los inserta en DuckDB.
    """
    files = list_parquet_for_processing_date(processing_date)
    if not files:
        print(f"No hay parquet para processing_date={processing_date}")
        return

    dfs = [load_parquet_to_dataframe(p) for p in files]
    full_df = pd.concat(dfs, ignore_index=True)

    conn = get_connection()
    ensure_table_exists(conn)

    # Insertamos con DuckDB
    conn.register("tmp_df", full_df)
    conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM tmp_df;")
    conn.unregister("tmp_df")

    print(f"Cargadas {len(full_df)} filas en tabla {TABLE_NAME}")


def run_load_leaderboard_raw_to_db():
    processing_date = date.today().strftime("%Y%m%d")
    load_into_duckdb(processing_date)


if __name__ == "__main__":
    run_load_leaderboard_raw_to_db()