"""
test_extract.py — Testes unitários para o módulo src/extract.py

Função testada: extract_weather_data(base_url: str) -> dict

O que a função faz (e o que precisamos garantir que continue funcionando):
    1. Faz GET na URL recebida
    2. Retorna {} se a rede falhar (ConnectionError, Timeout, etc.)
    3. Retorna {} se o status HTTP não for 200 (ex: 401, 404, 500)
    4. Retorna {} se o JSON vier vazio
    5. Salva o JSON em disco quando tudo ocorre bem
    6. Retorna o dicionário completo em caso de sucesso

Estratégia de Mock:
    - `requests.get` → simulamos a resposta da API sem tocar na internet
    - Escrita em disco → usamos a fixture `tmp_path` do pytest, que cria
      uma pasta temporária exclusiva para cada teste e a apaga depois.
      Isso evita poluir a pasta `data/` real do projeto durante os testes.

Por que não testamos a URL real da OpenWeather API?
    Testes que dependem de APIs externas são chamados de "testes de integração".
    Eles são lentos, frágeis (quebram se a API sair do ar) e precisam de
    chave de API configurada. O que queremos aqui são "testes unitários":
    rápidos, isolados e determinísticos (sempre produzem o mesmo resultado).
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from src.extract import extract_weather_data


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────

@pytest.fixture
def payload_valido() -> dict:
    """
    Simula um payload realista da OpenWeather API (versão simplificada).
    Este é o formato que nossa função espera receber — com os campos
    que o transform.py vai precisar depois.
    """
    return {
        "name": "Salvador",
        "cod": 200,
        "coord": {"lon": -38.5, "lat": -12.97},
        "weather": [{"id": 500, "main": "Rain", "description": "chuva leve", "icon": "10d"}],
        "main": {
            "temp": 26.48, "feels_like": 27.2,
            "temp_min": 25.0, "temp_max": 28.0,
            "pressure": 1012, "humidity": 85
        },
        "wind": {"speed": 6.82, "deg": 161, "gust": 7.54},
        "clouds": {"all": 57},
        "rain": {"1h": 0.37},
        "visibility": 6424,
        "dt": 1776880187,
        "sys": {"country": "BR", "sunrise": 1776847219, "sunset": 1776889466},
        "timezone": -10800,
        "id": 3450554,
        "base": "stations",
    }


@pytest.fixture
def mock_response_sucesso(payload_valido):
    """
    Cria um objeto Response "fantoche" que simula uma resposta 200 OK da API.

    MagicMock cria um objeto que aceita qualquer atributo/método sem erros.
    Configuramos os atributos que nossa função realmente acessa:
    - .status_code → 200
    - .json()      → retorna o payload real
    """
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = payload_valido
    return mock


# ─────────────────────────────────────────────
# TESTES
# ─────────────────────────────────────────────

class TestExtractWeatherData:

    def test_sucesso_retorna_dicionario_com_dados(self, mock_response_sucesso, tmp_path, monkeypatch):
        """
        CENÁRIO: API responde 200 OK com dados válidos.
        ESPERADO: Função retorna o dicionário com os dados da cidade.

        `monkeypatch` é outra forma de substituir funções — mais seguro em alguns
        contextos pois desfaz a substituição automaticamente após o teste.
        Aqui usamos patch() via context manager para manter consistência com os outros testes.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            # Redirecionamos o arquivo de saída para a pasta temporária do pytest
            with patch("src.extract.Path") as mock_path:
                mock_path.return_value.__truediv__ = lambda s, o: tmp_path / o
                mock_path.return_value.parent.parent.__truediv__ = lambda s, o: tmp_path
                resultado = extract_weather_data("http://url-falsa.com")

        assert isinstance(resultado, dict)
        assert resultado.get("name") == "Salvador"

    def test_sucesso_salva_arquivo_em_disco(self, mock_response_sucesso, payload_valido):
        """
        CENÁRIO: API responde com sucesso.
        ESPERADO: json.dump é chamado 1 vez com os dados corretos.

        Ao invés de tentar criar um arquivo real (o que causou PermissionError
        no Windows com o mock de Path), verificamos se a função de escrita
        foi invocada com os dados esperados. Testamos a INTENÇÃO de salvar,
        não o sistema de arquivos do SO.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            # Mockamos Path para não precisar criar diretórios reais
            with patch("src.extract.Path"):
                # Mockamos json.dump para interceptar a chamada de escrita
                with patch("src.extract.json.dump") as mock_dump:
                    extract_weather_data("http://url-falsa.com")

        # Valida que json.dump foi chamado exatamente 1 vez
        mock_dump.assert_called_once()

        # O primeiro argumento passado ao json.dump deve ser os dados da cidade
        dados_passados = mock_dump.call_args[0][0]  # args[0] = primeiro argumento posicional
        assert dados_passados.get("name") == "Salvador"

    def test_erro_de_rede_retorna_dict_vazio(self):
        """
        CENÁRIO: Internet cai durante a requisição (ConnectionError).
        ESPERADO: Função retorna {} SEM lançar exceção (pipeline não quebra).

        Este é o cenário mais crítico em produção: o Airflow está
        agendado para rodar às 08h e a internet cai às 07h59.
        O sistema deve falhar com dignidade (graceful degradation).

        CORREÇÃO: usamos requests.exceptions.RequestException (não Exception genérico),
        porque o extract.py só trata esse tipo específico de erro de rede.
        Lançar Exception genérico não seria capturado pelo `except RequestException`.
        """
        import requests as req_lib
        with patch("src.extract.requests.get",
                   side_effect=req_lib.exceptions.ConnectionError("Connection refused")):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_status_401_retorna_dict_vazio(self):
        """
        CENÁRIO: Chave de API inválida ou expirada (HTTP 401 Unauthorized).
        ESPERADO: Retorna {} sem tentar parsear o corpo da resposta.

        Um corpo de resposta 401 geralmente vem em HTML/texto, não JSON.
        Se tentarmos .json() nele, quebraria. Nossa função checa o status
        ANTES de parsear — este teste valida essa proteção.
        """
        mock = MagicMock()
        mock.status_code = 401
        mock.text = "Invalid API key"

        with patch("src.extract.requests.get", return_value=mock):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_status_404_retorna_dict_vazio(self):
        """
        CENÁRIO: Cidade não encontrada na API (HTTP 404 Not Found).
        ESPERADO: Retorna {} sem quebrar.
        """
        mock = MagicMock()
        mock.status_code = 404
        mock.text = "city not found"

        with patch("src.extract.requests.get", return_value=mock):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_status_500_retorna_dict_vazio(self):
        """
        CENÁRIO: Servidor da OpenWeather está com erro interno (HTTP 500).
        ESPERADO: Retorna {} sem quebrar.
        """
        mock = MagicMock()
        mock.status_code = 500
        mock.text = "Internal Server Error"

        with patch("src.extract.requests.get", return_value=mock):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_resposta_json_vazia_retorna_dict_vazio(self):
        """
        CENÁRIO: API responde 200, mas o corpo JSON vem vazio (dict vazio).
        ESPERADO: Retorna {} (nossa função checa `if not data`).

        Raro, mas acontece em APIs instáveis. Melhor tratar do que deixar
        o transform.py explodir tentando achatar um dict sem campos.
        """
        mock = MagicMock()
        mock.status_code = 200
        mock.json.return_value = {}  # Corpo vazio

        with patch("src.extract.requests.get", return_value=mock):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_timeout_retorna_dict_vazio(self):
        """
        CENÁRIO: API demora mais de 10 segundos para responder (Timeout).
        ESPERADO: Retorna {} sem travar o pipeline indefinidamente.

        O timeout=10 que definimos no extract.py garante que a função
        nunca fique pendurada por mais de 10 segundos. Este teste valida
        que a exceção de timeout é tratada corretamente.
        """
        import requests as req_lib
        with patch("src.extract.requests.get",
                   side_effect=req_lib.exceptions.Timeout("Timeout!")):
            resultado = extract_weather_data("http://url-falsa.com")

        assert resultado == {}

    def test_retorno_contem_campo_name(self, mock_response_sucesso):
        """
        CENÁRIO: Extração bem-sucedida.
        ESPERADO: O dict retornado contém o campo 'name' (cidade).

        Este teste valida o CONTRATO da função com o transform.py:
        garantimos que o campo essencial para identificar a cidade está presente.
        """
        with patch("src.extract.requests.get", return_value=mock_response_sucesso):
            with patch("src.extract.Path"):  # Ignora a escrita em disco
                resultado = extract_weather_data("http://url-falsa.com")

        assert "name" in resultado
