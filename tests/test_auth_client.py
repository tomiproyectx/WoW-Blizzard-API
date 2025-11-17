from pathlib import Path

import pytest

from tp2025.blizzard_api import auth_client as ac


class DummyResponse:
    def __init__(self, json_data):
        self._json_data = json_data

    def raise_for_status(self):
        # Simula response 200 sin error
        return None

    def json(self):
        return self._json_data


def test_get_token_writes_token_file(tmp_path, monkeypatch):
    # Mock de requests.post
    def fake_post(url, data, auth, timeout):
        assert "oauth/token" in url
        assert data == {"grant_type": "client_credentials"}
        return DummyResponse({"access_token": "FAKE_TOKEN_123"})

    monkeypatch.setattr(ac.requests, "post", fake_post)

    # Redirigimos TOKEN_FILE a un archivo temporal
    old_token_file = ac.TOKEN_FILE
    fake_token_file = tmp_path / ".blizzard_access_token"
    ac.TOKEN_FILE = fake_token_file

    client = ac.BlizzardAuthClient(
        client_id="dummy_id",
        client_secret="dummy_secret",
        region="us",
    )

    try:
        token = client.get_token()
        assert token == "FAKE_TOKEN_123"
        # Verificamos que se escribió el archivo con el token
        assert fake_token_file.exists()
        assert fake_token_file.read_text() == "FAKE_TOKEN_123"
    finally:
        # Restaurar el valor original para no romper otros tests
        ac.TOKEN_FILE = old_token_file


def test_load_token_from_file_returns_none_if_missing(tmp_path, monkeypatch):
    # Apuntamos TOKEN_FILE a un archivo que no existe
    old_token_file = ac.TOKEN_FILE
    ac.TOKEN_FILE = tmp_path / ".nonexistent_token"

    try:
        token = ac.load_token_from_file()
        assert token is None
    finally:
        ac.TOKEN_FILE = old_token_file