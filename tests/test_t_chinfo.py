import pandas as pd
from typing import Any, Dict, List, Tuple

from tp2025.services.ch_profile_client import (
    normalize_profile_row,
    build_profiles_dataframe,
)


def test_normalize_profile_row_basic():
    meta: Dict[str, Any] = {
        "char_id": 201902421,
        "char_name": "Manongauz",
        "slug_name": "demon-soul",
        "bracket_id": "3v3",
        "season_id": 40,
        "fecha_proceso": "20251117",
    }

    payload: Dict[str, Any] = {
        "id": 201902421,
        "name": "Manongauz",
        "faction": {"name": "Alliance"},
        "character_class": {"name": "Paladin"},
        "active_spec": {"name": "Retribution"},
        "average_item_level": 720,
        "equipped_item_level": 684,
    }

    row = normalize_profile_row(meta, payload)

    assert row["id"] == 201902421
    assert row["name"] == "Manongauz"
    assert row["realm_slug"] == "demon-soul"
    assert row["faction"] == "Alliance"
    assert row["class"] == "Paladin"
    assert row["spec"] == "Retribution"
    assert row["a_ilvl"] == 720
    assert row["e_ilvl"] == 684
    assert row["fecha_proceso"] == "20251117"


def test_build_profiles_dataframe_filters_none_payloads():
    meta_ok = {
        "char_id": 1,
        "char_name": "Testchar",
        "slug_name": "some-realm",
        "bracket_id": "2v2",
        "season_id": 40,
        "fecha_proceso": "20251117",
    }
    payload_ok = {
        "id": 1,
        "name": "Testchar",
        "faction": {"name": "Horde"},
        "character_class": {"name": "Warrior"},
        "active_spec": {"name": "Arms"},
        "average_item_level": 500,
        "equipped_item_level": 498,
    }

    meta_and_payloads: List[Tuple[Dict[str, Any], Dict[str, Any] | None]] = [
        (meta_ok, payload_ok),
        # Este debería descartarse
        (
            {
                "char_id": 2,
                "char_name": "NoPayload",
                "slug_name": "another-realm",
                "bracket_id": "3v3",
                "season_id": 40,
                "fecha_proceso": "20251117",
            },
            None,
        ),
    ]

    df = build_profiles_dataframe(meta_and_payloads)

    assert isinstance(df, pd.DataFrame)
    # Solo debe quedar 1 fila (la que tiene payload)
    assert df.shape[0] == 1
    row = df.iloc[0]
    assert row["id"] == 1
    assert row["name"] == "Testchar"
    assert row["realm_slug"] == "some-realm"