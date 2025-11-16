from __future__ import annotations

import os

DEFAULT_REGION: str = os.getenv("BLIZZARD_REGION", "us")
DEFAULT_LOCALE: str = os.getenv("BLIZZARD_LOCALE", "en_US")

# Namespaces separados por tipo de API
# Para PvP / data:
DEFAULT_PVP_NAMESPACE: str = os.getenv("BLIZZARD_PVP_NAMESPACE", "dynamic-us")
# Para character profile:
DEFAULT_PROFILE_NAMESPACE: str = os.getenv("BLIZZARD_PROFILE_NAMESPACE", "profile-us")


def get_base_url() -> str:
    """
    Devuelve la base URL de la API de Blizzard para la región configurada.
    """
    return f"https://{DEFAULT_REGION}.api.blizzard.com"


# ==========================
# PvP SEASON / LEADERBOARD
# ==========================

def get_pvp_season_index_url(
    *,
    namespace: str | None = None,
    locale: str | None = None,
) -> str:
    """
    /data/wow/pvp-season/index

    Ejemplo:
    https://us.api.blizzard.com/data/wow/pvp-season/index?namespace=dynamic-us&locale=en_US
    """
    base = get_base_url()
    ns = namespace or DEFAULT_PVP_NAMESPACE
    loc = locale or DEFAULT_LOCALE
    return f"{base}/data/wow/pvp-season/index?namespace={ns}&locale={loc}"


def get_pvp_leaderboard_url(
    *,
    season_id: int,
    bracket: str,
    namespace: str | None = None,
    locale: str | None = None,
) -> str:
    """
    /data/wow/pvp-season/{pvpSeasonId}/pvp-leaderboard/{pvpBracket}

    Ejemplo:
    https://us.api.blizzard.com/data/wow/pvp-season/40/pvp-leaderboard/3v3?namespace=dynamic-us&locale=en_US
    """
    base = get_base_url()
    ns = namespace or DEFAULT_PVP_NAMESPACE
    loc = locale or DEFAULT_LOCALE
    return (
        f"{base}/data/wow/pvp-season/{season_id}/pvp-leaderboard/{bracket}"
        f"?namespace={ns}&locale={loc}"
    )


# ==========================
# CHARACTER PROFILE
# ==========================

def _normalize_character_name(character_name: str) -> str:
    """
    La API espera el nombre en minúsculas.
    """
    return character_name.lower()


def get_character_profile_url(
    *,
    realm_slug: str,
    character_name: str,
    namespace: str | None = None,
    locale: str | None = None,
) -> str:
    """
    /profile/wow/character/{realmSlug}/{characterName}

    Ejemplo:
    https://us.api.blizzard.com/profile/wow/character/{realmSlug}/{characterName}?namespace=profile-us&locale=en_US
    """
    base = get_base_url()
    ns = namespace or DEFAULT_PROFILE_NAMESPACE
    loc = locale or DEFAULT_LOCALE
    char = _normalize_character_name(character_name)

    return (
        f"{base}/profile/wow/character/{realm_slug}/{char}"
        f"?namespace={ns}&locale={loc}"
    )


def get_character_professions_url(
    *,
    realm_slug: str,
    character_name: str,
    namespace: str | None = None,
    locale: str | None = None,
) -> str:
    """
    /profile/wow/character/{realmSlug}/{characterName}/professions
    """
    base = get_base_url()
    ns = namespace or DEFAULT_PROFILE_NAMESPACE
    loc = locale or DEFAULT_LOCALE
    char = _normalize_character_name(character_name)

    return (
        f"{base}/profile/wow/character/{realm_slug}/{char}/professions"
        f"?namespace={ns}&locale={loc}"
    )


def get_character_achievements_url(
    *,
    realm_slug: str,
    character_name: str,
    namespace: str | None = None,
    locale: str | None = None,
) -> str:
    """
    /profile/wow/character/{realmSlug}/{characterName}/achievements
    """
    base = get_base_url()
    ns = namespace or DEFAULT_PROFILE_NAMESPACE
    loc = locale or DEFAULT_LOCALE
    char = _normalize_character_name(character_name)

    return (
        f"{base}/profile/wow/character/{realm_slug}/{char}/achievements"
        f"?namespace={ns}&locale={loc}"
    )
