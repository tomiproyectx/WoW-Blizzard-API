from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.blizzard_api.endpoints import get_character_profile_url
from tp2025.blizzard_api.auth_client import load_token_from_file

MAX_WORKERS = 8


# ===== Token =====

def get_bearer_token() -> str:
    token = load_token_from_file()
    if not token:
        raise RuntimeError(
            "No se encontró un token en .blizzard_access_token. "
            "Generá primero un token válido con el script de autenticación."
        )
    return token


# ===== Requests concurrentes =====

def fetch_single_character_profile(
    session: requests.Session,
    token: str,
    realm_slug: str,
    character_name: str,
) -> Dict[str, Any] | None:
    url = get_character_profile_url(
        realm_slug=realm_slug,
        character_name=character_name,
    )
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = session.get(url, headers=headers, timeout=10)
    except requests.RequestException as exc:
        print(
            f"[ch_profile_client] Error de conexión para {realm_slug}/{character_name}: {exc}",
            file=sys.stderr,
        )
        return None

    if resp.status_code != 200:
        print(
            f"[ch_profile_client] Status {resp.status_code} para {realm_slug}/{character_name} - url={url}",
            file=sys.stderr,
        )
        return None

    return resp.json()


def fetch_profiles_concurrently(
    chars_df: pd.DataFrame,
    token: str,
    max_workers: int = MAX_WORKERS,
) -> List[Tuple[Dict[str, Any], Dict[str, Any] | None]]:
    """
    chars_df: columnas mínimas:
      - char_id
      - char_name
      - slug_name
      - bracket_id
      - season_id
      - fecha_proceso
    """
    records = chars_df.to_dict("records")
    results: List[Tuple[Dict[str, Any], Dict[str, Any] | None]] = []

    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_meta = {}
            for rec in records:
                meta = {
                    "char_id": rec["char_id"],
                    "char_name": rec["char_name"],
                    "slug_name": rec["slug_name"],
                    "bracket_id": rec["bracket_id"],
                    "season_id": rec["season_id"],
                    "fecha_proceso": rec["fecha_proceso"],
                }
                fut = executor.submit(
                    fetch_single_character_profile,
                    session,
                    token,
                    rec["slug_name"],
                    rec["char_name"].lower(),
                )
                future_to_meta[fut] = meta

            for fut in as_completed(future_to_meta):
                meta = future_to_meta[fut]
                payload = fut.result()
                results.append((meta, payload))

    return results


# ===== Normalización =====

def normalize_profile_row(
    meta: Dict[str, Any],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    faction = (payload.get("faction") or {}).get("name")
    char_class = (payload.get("character_class") or {}).get("name")
    spec = (payload.get("active_spec") or {}).get("name")

    row: Dict[str, Any] = {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "realm_slug": meta["slug_name"],
        "faction": faction,
        "class": char_class,
        "spec": spec,
        "a_ilvl": payload.get("average_item_level"),
        "e_ilvl": payload.get("equipped_item_level"),
        # trazabilidad mínima
        "fecha_proceso": meta["fecha_proceso"],
    }
    return row



def build_profiles_dataframe(
    meta_and_payloads: List[Tuple[Dict[str, Any], Dict[str, Any] | None]]
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for meta, payload in meta_and_payloads:
        if not payload:
            print(
                f"[ch_profile_client] Sin payload para {meta['slug_name']}/{meta['char_name']}",
                file=sys.stderr,
            )
            continue
        rows.append(normalize_profile_row(meta, payload))

    if not rows:
        raise RuntimeError("No se pudo construir ningún registro de perfil de personaje.")

    return pd.DataFrame(rows)