from unittest.mock import MagicMock, patch

import pytest
import requests as req_lib

from src.extract import _save_local_fallback, extract_weather_data


@pytest.fixture
def payload_valido() -> dict:
    """
    Simula um payload realista da Open-Meteo API.
    Este é o formato que nossa função espera receber — com os campos
    que o transform.py vai precisar depois.
    """
    return {
        "latitude": -12.9711,
        "longitude": -38.5108,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": -10800,
        "timezone": "America/Bahia",
        "timezone_abbreviation": "-03",
        "elevation": 10.0,
        "current_units": {
            "time": "unixtime",
            "interval": "seconds",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "rain": "mm",
            "weather_code": "wmo code",
            "wind_speed_10m": "km/h"
        },
        "current": {
            "time": 1714953600,
            "interval": 900,
            "temperature_2m": 27.5,
            "relative_humidity_2m": 82,
            "rain": 0.0,
            "weather_code": 3,
            "wind_speed_10m": 18.2
        }
    }


@pytest.fixture
def mock_response_sucesso(payload_valido):
    """
    Cria um objeto Response "fantoche" que simula uma resposta 200 OK da API.
    raise_for_status() não faz nada (simula status 2xx).
    """
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = payload_valido
    mock.raise_for_status.return_value = None  # Não levanta exceção
    return mock


def _make_error_response(status_code: int, text: str = "error"):
    """Cria um mock de resposta HTTP com erro que levanta HTTPError em raise_for_status."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = text

    http_err = req_lib.exceptions.HTTPError(response=mock)
    mock.raise_for_status.side_effect = http_err
    return mock


# ─────────────────────────────────────────────
# TESTES
# ─────────────────────────────────────────────


class TestExtractWeatherData:
    def test_sucesso_retorna_dados_e_object_key(
        self, mock_response_sucesso, payload_valido
    ):
        """
        CENÁRIO: API responde 200 OK e upload ao MinIO funciona.
        ESPERADO: Retorna (dict_com_dados, "alguma/chave.json").
        """
        bronze_key = "weather_data/2026-05-01/10-00-00_salvador.json"

        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            with patch(
                "src.extract.upload_to_bronze", return_value=bronze_key
            ) as mock_upload:
                dados, key = extract_weather_data("http://url-falsa.com")

        assert isinstance(dados, dict)
        assert "current" in dados
        assert key == bronze_key
        mock_upload.assert_called_once()

    def test_sucesso_chama_upload_to_bronze_com_dados_corretos(
        self, mock_response_sucesso, payload_valido
    ):
        """
        CENÁRIO: Extração bem-sucedida.
        ESPERADO: upload_to_bronze é chamado com o dicionário retornado pela API
        e com city='Salvador'.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            with patch(
                "src.extract.upload_to_bronze", return_value="chave.json"
            ) as mock_upload:
                extract_weather_data("http://url-falsa.com")

        call_kwargs = mock_upload.call_args
        dados_enviados = (
            call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1].get("data")
        )
        assert "current" in dados_enviados
        assert call_kwargs[1].get("city") == "Salvador"

    def test_minio_indisponivel_retorna_dados_e_none(self, mock_response_sucesso):
        """
        CENÁRIO: API da Open-Meteo responde OK, mas o MinIO está fora do ar.
        ESPERADO: Retorna (dados, None) — dados chegaram, mas não foram persistidos.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            with patch("src.extract.upload_to_bronze", return_value=None):
                dados, key = extract_weather_data("http://url-falsa.com")

        assert isinstance(dados, dict)
        assert "current" in dados
        assert key is None

    def test_erro_de_rede_retorna_tuple_vazio(self):
        """
        CENÁRIO: Internet cai durante a requisição (ConnectionError).
        ESPERADO: Retorna ({}, None) SEM lançar exceção (pipeline não quebra).
        """
        with patch(
            "src.extract.requests.get",
            side_effect=req_lib.exceptions.ConnectionError("Connection refused"),
        ):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_timeout_retorna_tuple_vazio(self):
        """
        CENÁRIO: API demora mais de 10 segundos para responder (Timeout).
        ESPERADO: Retorna ({}, None) sem travar o pipeline indefinidamente.
        """
        with patch(
            "src.extract.requests.get",
            side_effect=req_lib.exceptions.Timeout("Timeout!"),
        ):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_status_400_retorna_tuple_vazio(self):
        """
        CENÁRIO: Erro na chamada da API (ex: coordenadas faltantes - HTTP 400).
        ESPERADO: Retorna ({}, None).
        """
        mock = _make_error_response(400, "Bad Request")

        with patch("src.extract.requests.get", return_value=mock):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_status_500_retorna_tuple_vazio(self):
        """
        CENÁRIO: Servidor da Open-Meteo está com erro interno (HTTP 500).
        ESPERADO: Retorna ({}, None).
        """
        mock = _make_error_response(500, "Internal Server Error")

        with patch("src.extract.requests.get", return_value=mock):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_resposta_json_vazia_retorna_tuple_vazio(self):
        """
        CENÁRIO: API responde 200, mas o corpo JSON vem vazio (dict vazio).
        ESPERADO: Retorna ({}, None).
        """
        mock = MagicMock()
        mock.raise_for_status.return_value = None
        mock.json.return_value = {}

        with patch("src.extract.requests.get", return_value=mock):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_resposta_sem_bloco_current_retorna_tuple_vazio(self):
        """
        CENÁRIO: API responde 200, mas sem a tag 'current'.
        ESPERADO: Retorna ({}, None) (defesa contra JSON malformado).
        """
        mock = MagicMock()
        mock.raise_for_status.return_value = None
        mock.json.return_value = {"latitude": -12.0}

        with patch("src.extract.requests.get", return_value=mock):
            dados, key = extract_weather_data("http://url-falsa.com")

        assert dados == {}
        assert key is None

    def test_retorno_contem_bloco_current(self, mock_response_sucesso):
        """
        CENÁRIO: Extração bem-sucedida.
        ESPERADO: O dict retornado contém o bloco 'current'.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            with patch("src.extract.upload_to_bronze", return_value="chave.json"):
                dados, _ = extract_weather_data("http://url-falsa.com")

        assert "current" in dados


class TestSaveLocalFallback:
    def test_salva_arquivo_em_disco(self, tmp_path, payload_valido):
        """
        CENÁRIO: Desenvolvedor quer salvar fallback local.
        ESPERADO: Arquivo JSON é criado no caminho especificado.
        """

        destino = tmp_path / "weather_data.json"

        _save_local_fallback(payload_valido, destino)

        assert destino.exists()

    def test_conteudo_do_arquivo_e_valido(self, tmp_path, payload_valido):
        """
        CENÁRIO: Fallback salvo em disco.
        ESPERADO: Conteúdo do arquivo é JSON válido com os dados corretos.
        """
        import json

        destino = tmp_path / "weather_data.json"
        _save_local_fallback(payload_valido, destino)

        conteudo = json.loads(destino.read_text(encoding="utf-8"))
        assert conteudo.get("latitude") == -12.9711

    def test_cria_diretorio_pai_se_nao_existir(self, tmp_path, payload_valido):
        """
        CENÁRIO: Diretório pai do arquivo não existe.
        ESPERADO: Função cria os diretórios necessários antes de salvar.
        """

        destino = tmp_path / "subdir" / "novo" / "weather_data.json"
        _save_local_fallback(payload_valido, destino)

        assert destino.exists()
