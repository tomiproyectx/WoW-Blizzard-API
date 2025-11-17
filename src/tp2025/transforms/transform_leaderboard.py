from __future__ import annotations

from datetime import date

from tp2025.io.load_localdb import get_connection, run_sql

RAW_TABLE = "raw_pvp_leaderboard"
CUR_TABLE = "cur_pvp_leaderboard"


def create_cur_leaderboard(processing_date: str | None = None) -> None:
    """
    Construye la tabla CUR a partir de la tabla RAW para una fecha de proceso dada.

    - Toma datos de raw_pvp_leaderboard.
    - Filtra por fecha_proceso (por defecto, la fecha de hoy en formato YYYYMMDD).
    - Castea tipos y renombra columnas según el modelo de negocio.
    - Crea o reemplaza la tabla cur_pvp_leaderboard.
    """
    if processing_date is None:
        processing_date = date.today().strftime("%Y%m%d")

    conn = get_connection()
    try:
        query = f"""
        CREATE OR REPLACE TABLE {CUR_TABLE} AS
        SELECT 
            CAST(id AS BIGINT)      AS char_id,
            name                    AS char_name,
            slug                    AS slug_name,
            faction                 AS faction_type,
            CAST(rank AS INT)       AS ranking,
            CAST(rating AS INT)     AS rating,
            CAST(played AS INT)     AS games_played,
            CAST(won AS INT)        AS games_won,
            CAST(lost AS INT)       AS games_lost,
            bracket                 AS bracket_id,
            CAST(s_id AS INT)       AS season_id,
            fecha_proceso
        FROM {RAW_TABLE}
        WHERE fecha_proceso = '{processing_date}';
        """
        run_sql(conn, query)
        print(
            f"[transform_leaderboard] Tabla {CUR_TABLE} generada "
            f"para fecha_proceso={processing_date}"
        )
    finally:
        conn.close()