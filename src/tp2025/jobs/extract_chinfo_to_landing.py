from __future__ import annotations

from datetime import date
from pathlib import Path

import sys
import pandas as pd


THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"

from tp2025.services.character_selection import get_top_pvp_characters
from tp2025.services.ch_profile_client import (
    get_bearer_token,
    fetch_profiles_concurrently,
    build_profiles_dataframe,
)


def get_processing_date_str() -> str:
    return date.today().strftime("%Y%m%d")


def save_profiles_to_parquet(
    df: pd.DataFrame,
    processing_date: str,
) -> Path:
    """
    Guarda un parquet de snapshot de personajes:
    ch_profile_{processing_date}.parquet
    """
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"ch_profile_{processing_date}.parquet"
    path = LANDING_DIR / filename
    df.to_parquet(path, index=False)
    return path


def run_extract_chinfo_to_landing() -> None:
    processing_date = get_processing_date_str()
    print(f"[extract_chinfo_to_landing] Fecha de proceso: {processing_date}")

    # 1) Selección de personajes desde cur_pvp_leaderboard
    chars_df = get_top_pvp_characters(processing_date, limit_total=500)
    print(
    f"[extract_chinfo_to_landing] Personajes seleccionados: "
    f"{len(chars_df)} filas (máx 200, únicos por char_id)."
    )


    # 2) Token
    token = get_bearer_token()

    # 3) Requests concurrentes al endpoint de profile
    meta_and_payloads = fetch_profiles_concurrently(chars_df, token)

    # 4) Normalización
    df_profiles = build_profiles_dataframe(meta_and_payloads)

    # 5) Guardado a landing
    path = save_profiles_to_parquet(df_profiles, processing_date)

    print(
        f"[extract_chinfo_to_landing] Guardado {len(df_profiles)} perfiles "
        f"de personajes en: {path}"
    )


if __name__ == "__main__":
    run_extract_chinfo_to_landing()