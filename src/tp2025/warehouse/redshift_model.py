from __future__ import annotations

from datetime import date

import pandas as pd
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values


DIM_SEASON_DDL = """
CREATE TABLE IF NOT EXISTS dim_season (
    season_id INTEGER PRIMARY KEY,
    season_name VARCHAR(100)
);
"""

DIM_BRACKET_DDL = """
CREATE TABLE IF NOT EXISTS dim_bracket (
    bracket_id VARCHAR(10) PRIMARY KEY,
    bracket_name VARCHAR(50)
);
"""

DIM_CHARACTER_SCD2_DDL = """
CREATE TABLE IF NOT EXISTS dim_character_scd2 (
    ch_sk           BIGINT IDENTITY(1,1),
    char_id         BIGINT NOT NULL,
    char_name       VARCHAR(50),
    slug_name       VARCHAR(50),
    faction_type    VARCHAR(20),
    class_name      VARCHAR(50),
    current_spec    VARCHAR(50),
    fecha_proceso   VARCHAR(8),
    CONSTRAINT pk_dim_character PRIMARY KEY (ch_sk)
);
"""

FACT_PVP_LEADERBOARD_DDL = """
CREATE TABLE IF NOT EXISTS fact_pvp_leaderboard_snapshot (
    snapshot_date  DATE NOT NULL,
    char_id        BIGINT NOT NULL,
    season_id      INTEGER NOT NULL,
    bracket_id     VARCHAR(10) NOT NULL,
    rating         INTEGER,
    ranking        INTEGER,
    games_played   INTEGER,
    games_won      INTEGER,
    games_lost     INTEGER
);
"""


def create_tables(conn: PgConnection) -> None:
    """
    Crea las tablas del modelo estrella en Redshift si no existen.
    """
    ddls = [
        DIM_SEASON_DDL,
        DIM_BRACKET_DDL,
        DIM_CHARACTER_SCD2_DDL,
        FACT_PVP_LEADERBOARD_DDL,
    ]
    with conn.cursor() as cur:
        for ddl in ddls:
            cur.execute(ddl)
    conn.commit()


def load_dim_character_scd2(conn: PgConnection, df_chinfo: pd.DataFrame) -> None:
    """
    Carga SCD2 "simple" de personajes en modo bulk:
    - Appendea una fila por personaje para la fecha_proceso dada.
    - Usa execute_values para evitar 1 INSERT por fila.
    """
    if df_chinfo.empty:
        return

    # Armamos la lista de tuplas con los valores
    rows = [
        (
            int(row["char_id"]),
            row["char_name"],
            row["slug_name"],
            row["faction_type"],
            row.get("class_name"),
            row.get("current_spec"),
            row["fecha_proceso"],  # 'YYYYMMDD'
        )
        for _, row in df_chinfo.iterrows()
    ]

    sql = """
        INSERT INTO dim_character_scd2 (
            char_id,
            char_name,
            slug_name,
            faction_type,
            class_name,
            current_spec,
            fecha_proceso
        )
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()



def load_fact_leaderboard(
    conn: PgConnection,
    df_leaderboard: pd.DataFrame,
    snapshot_date: date,
) -> None:
    """
    Carga la tabla de hechos de snapshot del leaderboard en modo bulk.
    """
    if df_leaderboard.empty:
        return

    rows = [
        (
            snapshot_date,
            int(row["char_id"]),
            int(row["season_id"]),
            row["bracket_id"],
            int(row["rating"]),
            int(row["ranking"]),
            int(row["games_played"]),
            int(row["games_won"]),
            int(row["games_lost"]),
        )
        for _, row in df_leaderboard.iterrows()
    ]

    sql = """
        INSERT INTO fact_pvp_leaderboard_snapshot (
            snapshot_date,
            char_id,
            season_id,
            bracket_id,
            rating,
            ranking,
            games_played,
            games_won,
            games_lost
        )
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

