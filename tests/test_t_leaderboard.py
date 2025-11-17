import pandas as pd
from pathlib import Path

from tp2025.jobs.extract_leaderboard_to_landing import (
    normalize_leaderboard_entries,
)
from tp2025.jobs.load_leaderboard_raw_to_db import (
    parse_metadata,
)


def test_normalize_leaderboard_entries_single_row():
    payload = {
        "entries": [
            {
                "character": {
                    "id": 252903401,
                    "name": "Lørdnick",
                    "realm": {
                        "key": {
                            "href": "https://us.api.blizzard.com/data/wow/realm/60?namespace=dynamic-us"
                        },
                        "id": 60,
                        "slug": "stormrage",
                    },
                },
                "faction": {"type": "HORDE"},
                "rank": 1,
                "rating": 2954,
                "season_match_statistics": {
                    "played": 217,
                    "won": 144,
                    "lost": 73,
                },
            }
        ]
    }

    df = normalize_leaderboard_entries(payload)

    # Tiene que haber exactamente una fila
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 1

    row = df.iloc[0]
    assert row["id"] == 252903401
    assert row["name"] == "Lørdnick"
    assert row["slug"] == "stormrage"
    assert row["faction"] == "HORDE"
    assert row["rank"] == 1
    assert row["rating"] == 2954
    assert row["played"] == 217
    assert row["won"] == 144
    assert row["lost"] == 73


def test_normalize_leaderboard_entries_empty_entries():
    payload = {"entries": []}

    df = normalize_leaderboard_entries(payload)

    # DataFrame vacío es un resultado válido
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 0



def test_parse_metadata_valid_filename():
    path = Path("pvp_leaderboard_s40_3v3_20251117.parquet")

    s_id, bracket, fecha = parse_metadata(path)

    assert s_id == 40
    assert bracket == "3v3"
    assert fecha == "20251117"