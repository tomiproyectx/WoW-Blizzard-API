import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_FILE = Path.cwd() / ".blizzard_access_token"


@dataclass
class BlizzardAuthClient:
    client_id: str
    client_secret: str
    region: str = "us"

    def get_token(self) -> str:
        """
        Obtiene un access token vía Client Credentials Flow.
        Además, lo guarda en un archivo local para que otros scripts puedan reutilizarlo.
        """
        url = f"https://{self.region}.battle.net/oauth/token"
        resp = requests.post(
            url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=10,
        )
        resp.raise_for_status()
        data: Dict[str, Any] = resp.json()
        token = data["access_token"]

        # Persistimos el token en un archivo de texto plano
        TOKEN_FILE.write_text(token)

        return token


def get_default_auth_client() -> BlizzardAuthClient:
    return BlizzardAuthClient(
        client_id=os.environ["BLIZZARD_CLIENT_ID"],
        client_secret=os.environ["BLIZZARD_CLIENT_SECRET"],
        region=os.getenv("BLIZZARD_REGION", "us"),
    )


def load_token_from_file() -> Optional[str]:
    """
    Devuelve el token guardado en el archivo local, si existe.
    No valida expiración ni nada sofisticado: es literal lo que haya en el archivo.
    """
    if not TOKEN_FILE.exists():
        return None
    token = TOKEN_FILE.read_text().strip()
    return token or None
