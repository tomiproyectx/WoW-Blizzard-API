from __future__ import annotations

from datetime import date
from typing import Optional
import sys
from pathlib import Path

# Ajuste de sys.path solo si ejecutás este script directamente
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.io.load_localdb import get_connection as get_duckdb_connection
from tp2025.warehouse.connect_redshift import get_connection as get_redshift_connection
from tp2025.warehouse.redshift_model import (
    create_tables,
    load_dim_character_scd2,
    load_fact_leaderboard,
)


def fetch_cur_chinfo(processing_date: str, duck_conn):
    query = f"""
        SELECT
            char_id,
            char_name,
            slug_name,
            faction_type,
            class_name,
            current_spec,
            fecha_proceso
        FROM wow_data.main.cur_chinfo
        WHERE fecha_proceso = '{processing_date}';
    """
    return duck_conn.execute(query).fetchdf()


def fetch_cur_leaderboard(processing_date: str, duck_conn):
    query = f"""
        SELECT
            char_id,
            char_name,
            slug_name,
            faction_type,
            ranking,
            rating,
            games_played,
            games_won,
            games_lost,
            bracket_id,
            season_id,
            fecha_proceso
        FROM wow_data.main.cur_pvp_leaderboard
        WHERE fecha_proceso = '{processing_date}';
    """
    return duck_conn.execute(query).fetchdf()


def main(processing_date: Optional[str] = None) -> None:
    """
    Carga diaria hacia Redshift:

    - Lee cur_chinfo y cur_pvp_leaderboard de DuckDB para una fecha de proceso.
    - Crea las tablas del modelo estrella si no existen.
    - Appendea snapshot de personajes en dim_character_scd2 (SCD2 simple por fecha_proceso).
    - Appendea snapshot del leaderboard en fact_pvp_leaderboard_snapshot.
    """
    if processing_date is None:
        processing_date = date.today().strftime("%Y%m%d")

    # 1) Leer datos desde DuckDB
    duck_conn = get_duckdb_connection()
    try:
        df_chinfo = fetch_cur_chinfo(processing_date, duck_conn)
        df_leaderboard = fetch_cur_leaderboard(processing_date, duck_conn)
    finally:
        duck_conn.close()

    # 2) Conectar a Redshift
    red_conn = get_redshift_connection()
    try:
        # Crear tablas del modelo estrella (dim + fact)
        create_tables(red_conn)

        # Cargar dimensión SCD2 simplificada
        load_dim_character_scd2(red_conn, df_chinfo)

        # Cargar fact de snapshot del leaderboard
        load_fact_leaderboard(red_conn, df_leaderboard, snapshot_date=date.today())
    finally:
        red_conn.close()


if __name__ == "__main__":
    main()
