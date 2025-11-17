from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.io.load_localdb import get_connection


def get_top_pvp_characters(processing_date: str, limit_total: int = 500) -> pd.DataFrame:
    """
    Devuelve hasta N personajes únicos (por char_id) desde cur_pvp_leaderboard
    para la fecha_proceso indicada.

    Lógica:
    - Calcula ranking por bracket (ROW_NUMBER particionado por bracket_id).
    - Para cada char_id, se queda con la mejor fila (menor ranking).
    - De esos personajes únicos, trae hasta limit_total, ordenados por ranking.
    """

    query = """
    WITH ranked AS (
        SELECT
            char_id,
            char_name,
            slug_name,
            bracket_id,
            season_id,
            fecha_proceso,
            ranking,
            games_won,
            games_lost
        FROM wow_data.main.cur_pvp_leaderboard
        WHERE fecha_proceso = ?
          AND bracket_id IN ('2v2', '3v3')
    ),
    per_bracket AS (
        SELECT
            char_id,
            char_name,
            slug_name,
            bracket_id,
            season_id,
            fecha_proceso,
            ROW_NUMBER() OVER (
                PARTITION BY bracket_id
                ORDER BY ranking, games_won DESC, games_lost ASC
            ) AS bracket_rank
        FROM ranked
    ),
    scored AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY char_id
                ORDER BY bracket_rank,
                         CASE bracket_id WHEN '3v3' THEN 1 ELSE 2 END
            ) AS rn_char
        FROM per_bracket
    ),
    best_per_char AS (
        SELECT
            char_id,
            char_name,
            slug_name,
            bracket_id,
            season_id,
            fecha_proceso,
            bracket_rank AS ranking
        FROM scored
        WHERE rn_char = 1
    )
    SELECT
        char_id,
        char_name,
        slug_name,
        bracket_id,
        season_id,
        fecha_proceso,
        ranking
    FROM best_per_char
    ORDER BY ranking, bracket_id
    LIMIT ?;
    """

    conn = get_connection()
    df = conn.execute(query, [processing_date, limit_total]).df()

    if df.empty:
        raise RuntimeError(
            f"No se encontraron filas en cur_pvp_leaderboard para fecha_proceso={processing_date}"
        )

    return df
