import json
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from src.storage import (
    _build_bronze_key,
    upload_to_bronze,
    download_from_bronze,
    upload_to_silver,
    download_from_silver,
    list_bronze_files,
    list_silver_files,
)

def _make_client_error(code: str) -> ClientError:
    """Cria um ClientError simulado com o código de erro especificado."""
    return ClientError(
        error_response={"Error": {"Code": code, "Message": "mocked error"}},
        operation_name="MockOperation",
    )


@pytest.fixture
def mock_s3_client():
    return MagicMock()


@pytest.fixture
def dados_brutos() -> dict:
    return {
        "name": "Salvador",
        "main": {"temp": 28.5, "humidity": 82},
        "wind": {"speed": 7.2},
    }


# ─────────────────────────────────────────────
# 1. TESTES: _build_bronze_key
# ─────────────────────────────────────────────

class TestBuildBronzeKey:

    def test_retorna_string_nao_vazia(self):
        key = _build_bronze_key("salvador")
        assert isinstance(key, str) and len(key) > 0

    def test_formato_particionado(self):
        """weather_data/YYYY-MM-DD/HH-MM-SS_<cidade>.json"""
        key = _build_bronze_key("salvador")
        partes = key.split("/")
        assert partes[0] == "weather_data"
        assert len(partes[1]) == 10        # YYYY-MM-DD
        assert partes[2].endswith("_salvador.json")

    def test_cidade_lowercased(self):
        key = _build_bronze_key("SALVADOR")
        assert "_salvador.json" in key

    def test_cidade_default_e_salvador(self):
        key = _build_bronze_key()
        assert "salvador" in key


# ─────────────────────────────────────────────
# 2. TESTES: upload_to_bronze
# ─────────────────────────────────────────────

class TestUploadToBronze:

    def test_retorna_object_key_em_sucesso(self, mock_s3_client, dados_brutos):
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.put_object.return_value = {}
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_bronze(dados_brutos, city="salvador")
        assert isinstance(resultado, str)
        assert "weather_data/" in resultado

    def test_dados_vazios_retornam_none(self, mock_s3_client):
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_bronze({})
        assert resultado is None
        mock_s3_client.put_object.assert_not_called()

    def test_client_error_retorna_none(self, mock_s3_client, dados_brutos):
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.put_object.side_effect = _make_client_error("AccessDenied")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_bronze(dados_brutos)
        assert resultado is None

    def test_cria_bucket_se_nao_existir(self, mock_s3_client, dados_brutos):
        mock_s3_client.head_bucket.side_effect = _make_client_error("404")
        mock_s3_client.create_bucket.return_value = {}
        mock_s3_client.put_object.return_value = {}
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_bronze(dados_brutos)
        mock_s3_client.create_bucket.assert_called_once()
        assert resultado is not None

    def test_body_e_json_serializado(self, mock_s3_client, dados_brutos):
        """put_object deve receber Body com bytes JSON válidos."""
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.put_object.return_value = {}
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            upload_to_bronze(dados_brutos, city="salvador")
        call_kwargs = mock_s3_client.put_object.call_args[1]
        dados_recuperados = json.loads(call_kwargs["Body"].decode("utf-8"))
        assert dados_recuperados["name"] == "Salvador"


# ─────────────────────────────────────────────
# 3. TESTES: download_from_bronze
# ─────────────────────────────────────────────

class TestDownloadFromBronze:

    def test_retorna_dicionario_em_sucesso(self, mock_s3_client, dados_brutos):
        json_bytes = json.dumps(dados_brutos).encode("utf-8")
        mock_s3_client.get_object.return_value = {"Body": MagicMock(read=lambda: json_bytes)}
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = download_from_bronze("weather_data/2026-04-30/10-00-00_salvador.json")
        assert isinstance(resultado, dict)
        assert resultado["name"] == "Salvador"

    def test_object_key_vazia_retorna_dict_vazio(self):
        assert download_from_bronze("") == {}

    def test_client_error_retorna_dict_vazio(self, mock_s3_client):
        mock_s3_client.get_object.side_effect = _make_client_error("NoSuchKey")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = download_from_bronze("chave_invalida.json")
        assert resultado == {}


# ─────────────────────────────────────────────
# 4. TESTES: upload_to_silver
# ─────────────────────────────────────────────

class TestUploadToSilver:

    def test_retorna_object_key_em_sucesso(self, mock_s3_client):
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.put_object.return_value = {}
        silver_key = "weather_silver/2026-04-30/10-00-00_salvador.json"
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_silver('[{"cidade": "Salvador"}]', silver_key)
        assert resultado == silver_key

    def test_json_vazio_retorna_none(self, mock_s3_client):
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_silver("", "chave.json")
        assert resultado is None
        mock_s3_client.put_object.assert_not_called()

    def test_client_error_retorna_none(self, mock_s3_client):
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.put_object.side_effect = _make_client_error("InternalError")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = upload_to_silver('[{"cidade": "Salvador"}]', "chave.json")
        assert resultado is None


# ─────────────────────────────────────────────
# 5. TESTES: download_from_silver
# ─────────────────────────────────────────────

class TestDownloadFromSilver:

    def test_retorna_string_json_em_sucesso(self, mock_s3_client):
        json_str = '[{"cidade": "Salvador", "temperatura_c": 28.5}]'
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: json_str.encode("utf-8"))
        }
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = download_from_silver("weather_silver/2026-04-30/10-00-00_salvador.json")
        assert isinstance(resultado, str)
        assert json.loads(resultado)[0]["cidade"] == "Salvador"

    def test_object_key_vazia_retorna_string_vazia(self):
        assert download_from_silver("") == ""

    def test_client_error_retorna_string_vazia(self, mock_s3_client):
        mock_s3_client.get_object.side_effect = _make_client_error("NoSuchKey")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = download_from_silver("chave_invalida.json")
        assert resultado == ""


# ─────────────────────────────────────────────
# 6. TESTES: list_bronze_files / list_silver_files
# ─────────────────────────────────────────────

class TestListFiles:

    def _mock_paginator(self, client, keys: list):
        page = {"Contents": [{"Key": k} for k in keys]} if keys else {}
        paginator = MagicMock()
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator

    def test_list_bronze_retorna_keys(self, mock_s3_client):
        keys = [
            "weather_data/2026-04-29/08-00-00_salvador.json",
            "weather_data/2026-04-30/08-00-00_salvador.json",
        ]
        self._mock_paginator(mock_s3_client, keys)
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = list_bronze_files()
        assert len(resultado) == 2

    def test_list_bronze_vazio_retorna_lista_vazia(self, mock_s3_client):
        self._mock_paginator(mock_s3_client, [])
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = list_bronze_files()
        assert resultado == []

    def test_list_bronze_client_error_retorna_lista_vazia(self, mock_s3_client):
        mock_s3_client.get_paginator.side_effect = _make_client_error("AccessDenied")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = list_bronze_files()
        assert resultado == []

    def test_list_silver_retorna_keys(self, mock_s3_client):
        keys = ["weather_silver/2026-04-30/08-00-00_salvador.json"]
        self._mock_paginator(mock_s3_client, keys)
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = list_silver_files()
        assert len(resultado) == 1

    def test_list_silver_client_error_retorna_lista_vazia(self, mock_s3_client):
        mock_s3_client.get_paginator.side_effect = _make_client_error("NoSuchBucket")
        with patch("src.storage._get_s3_client", return_value=mock_s3_client):
            resultado = list_silver_files()
        assert resultado == []
