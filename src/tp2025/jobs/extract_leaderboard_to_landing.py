from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List
from datetime import date

import pandas as pd
import requests


THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.blizzard_api.auth_client import load_token_from_file, get_default_auth_client
from tp2025.blizzard_api.endpoints import (
    get_pvp_season_index_url,
    get_pvp_leaderboard_url,
)

LANDING_DIR = PROJECT_ROOT / "data" / "landing"



def get_token() -> str:
    """
    Devuelve un access token válido.
    - Si existe .blizzard_access_token, lo usa.
    - Si no, usa client_credentials via BlizzardAuthClient y lo persiste en ese archivo.
    """
    token = load_token_from_file()
    if token:
        return token

    client = get_default_auth_client()
    return client.get_token()


def get_current_season_id(token: str) -> int:
    """
    Llama al endpoint de PvP Season Index y devuelve current_season.id.
    """
    url = get_pvp_season_index_url()
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data: Dict[str, Any] = resp.json()

    try:
        return int(data["current_season"]["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("No se pudo obtener 'current_season.id' del payload") from exc


def fetch_leaderboard_raw(token: str, season_id: int, bracket: str) -> Dict[str, Any]:
    """
    Llama al endpoint de PvP Leaderboard para un bracket dado
    y devuelve el payload JSON completo.
    """
    url = get_pvp_leaderboard_url(season_id=season_id, bracket=bracket)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_leaderboard_entries(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Toma el JSON del leaderboard y lo transforma al modelo raw:

    id, name, slug, faction, rank, rating, played, won, lost
    """
    entries: List[Dict[str, Any]] = payload.get("entries", [])
    rows: List[Dict[str, Any]] = []

    for e in entries:
        char = e.get("character", {}) or {}
        realm = (char.get("realm") or {}) if isinstance(char, dict) else {}
        stats = e.get("season_match_statistics", {}) or {}
        faction = e.get("faction", {}) or {}

        row = {
            "id": char.get("id"),
            "name": char.get("name"),
            "slug": realm.get("slug"),
            "faction": faction.get("type"),
            "rank": e.get("rank"),
            "rating": e.get("rating"),
            "played": stats.get("played"),
            "won": stats.get("won"),
            "lost": stats.get("lost"),
        }
        rows.append(row)

    return pd.DataFrame(rows)


def save_leaderboard_to_parquet(
    df: pd.DataFrame,
    season_id: int,
    bracket: str,
    processing_date: str,
) -> Path:
    """
    Guarda el DataFrame en data/landing como parquet.
    Nombre: pvp_leaderboard_s{season_id}_{bracket}_{processing_date}.parquet
    """
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"pvp_leaderboard_s{season_id}_{bracket}_{processing_date}.parquet"
    path = LANDING_DIR / filename
    df.to_parquet(path, index=False)
    return path



def run_extract_leaderboard_to_landing() -> None:
    """
    Orquestador de la etapa de extracción a landing:

    1) Obtiene token.
    2) Obtiene current_season_id.
    3) Extrae 2v2 y 3v3.
    4) Normaliza al modelo raw y guarda en parquet.
    """
    token = get_token()
    season_id = get_current_season_id(token)
    processing_date = date.today().strftime("%Y%m%d")

    for bracket in ("2v2", "3v3"):
        payload = fetch_leaderboard_raw(token, season_id, bracket)
        df = normalize_leaderboard_entries(payload)
        path = save_leaderboard_to_parquet(df, season_id, bracket, processing_date)
        print(
            f"[extract_leaderboard_to_landing] Guardado {len(df)} filas "
            f"para bracket={bracket} en: {path}"
        )


if __name__ == "__main__":
    run_extract_leaderboard_to_landing()
