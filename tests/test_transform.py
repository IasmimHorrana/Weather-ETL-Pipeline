import json
from unittest.mock import patch

import pandas as pd
import pytest

from src.transform import (
    calculate_risk_level,
    convert_timestamps_to_local,
    flatten_to_dataframe,
    load_from_bronze,
    load_raw_json,
    run_pipeline,
    save_silver_data,
    translate_weather_code,
    validate_rain_schema,
)


@pytest.fixture
def raw_json_completo() -> dict:
    """
    Simula o payload completo da Open-Meteo API.
    É o input da nossa Máquina 2.
    """
    return {
        "latitude": -12.9711,
        "longitude": -38.5108,
        "current": {
            "time": 1714953600,
            "temperature_2m": 26.48,
            "relative_humidity_2m": 85,
            "rain": 0.37,
            "weather_code": 61,
            "wind_speed_10m": 18.0,  # (18.0 / 3.6 = 5.0 m/s)
        },
    }


@pytest.fixture
def raw_json_sem_chuva(raw_json_completo) -> dict:
    """
    Mesmo payload, mas sem o campo 'rain' (dia ensolarado).
    """
    data = json.loads(json.dumps(raw_json_completo))
    data["current"].pop("rain", None)
    return data


@pytest.fixture
def df_achatado(raw_json_completo) -> pd.DataFrame:
    """DataFrame já achatado — input para as funções de transformação."""
    return flatten_to_dataframe(raw_json_completo)


@pytest.fixture
def df_silver(df_achatado) -> pd.DataFrame:
    """
    DataFrame passado por todas as etapas.
    Usado como base para testar calculate_risk_level e save_silver.
    """
    df = convert_timestamps_to_local(df_achatado)
    df = translate_weather_code(df)
    df = validate_rain_schema(df)
    return df


# ─────────────────────────────────────────────
# 1. TESTES: load_raw_json
# ─────────────────────────────────────────────


class TestLoadRawJson:
    def test_carrega_json_existente_com_sucesso(self, tmp_path, raw_json_completo):
        arquivo = tmp_path / "weather_data.json"
        arquivo.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        resultado = load_raw_json(arquivo)

        assert isinstance(resultado, dict)
        assert "current" in resultado

    def test_arquivo_inexistente_lanca_file_not_found(self, tmp_path):
        caminho_falso = tmp_path / "nao_existe.json"

        with pytest.raises(FileNotFoundError):
            load_raw_json(caminho_falso)

    def test_retorno_e_dicionario(self, tmp_path, raw_json_completo):
        arquivo = tmp_path / "weather_data.json"
        arquivo.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        resultado = load_raw_json(arquivo)

        assert isinstance(resultado, dict)


# ─────────────────────────────────────────────
# 2. TESTES: flatten_to_dataframe
# ─────────────────────────────────────────────


class TestFlattenToDataframe:
    def test_retorna_dataframe(self, raw_json_completo):
        resultado = flatten_to_dataframe(raw_json_completo)
        assert isinstance(resultado, pd.DataFrame)

    def test_dataframe_tem_exatamente_uma_linha(self, raw_json_completo):
        df = flatten_to_dataframe(raw_json_completo)
        assert len(df) == 1

    def test_extrai_campo_rain_quando_presente(self, raw_json_completo):
        df = flatten_to_dataframe(raw_json_completo)
        assert "chuva_1h_mm" in df.columns
        assert df["chuva_1h_mm"].iloc[0] == pytest.approx(0.37)

    def test_sem_rain_cria_coluna_zerada(self, raw_json_sem_chuva):
        df = flatten_to_dataframe(raw_json_sem_chuva)
        assert "chuva_1h_mm" in df.columns
        assert df["chuva_1h_mm"].iloc[0] == 0.0

    def test_campo_cidade_injetado_apos_flatten(self, raw_json_completo):
        df = flatten_to_dataframe(raw_json_completo)
        assert "cidade" in df.columns
        assert df["cidade"].iloc[0] == "Salvador"

    def test_conversao_do_vento(self, raw_json_completo):
        df = flatten_to_dataframe(raw_json_completo)
        assert "vento_velocidade_ms" in df.columns
        # 18.0 km/h / 3.6 = 5.0 m/s
        assert df["vento_velocidade_ms"].iloc[0] == pytest.approx(5.0)


# ─────────────────────────────────────────────
# 3. TESTES: convert_timestamps_to_local
# ─────────────────────────────────────────────


class TestConvertTimestampsToLocal:
    def test_coluna_data_hora_vira_datetime(self, df_achatado):
        df = convert_timestamps_to_local(df_achatado)
        assert pd.api.types.is_datetime64_any_dtype(df["data_hora"])

    def test_fuso_horario_aplicado_e_america_bahia(self, df_achatado):
        df = convert_timestamps_to_local(df_achatado)
        tz_name = str(df["data_hora"].dt.tz)
        assert "Bahia" in tz_name or "America" in tz_name

    def test_nao_modifica_dataframe_original(self, df_achatado):
        dt_original = df_achatado["data_hora"].iloc[0]
        convert_timestamps_to_local(df_achatado)
        assert df_achatado["data_hora"].iloc[0] == dt_original

    def test_colunas_ausentes_sao_ignoradas_sem_erro(self):
        df_sem_timestamps = pd.DataFrame(
            [{"cidade": "Salvador", "temperatura_c": 26.0}]
        )
        resultado = convert_timestamps_to_local(df_sem_timestamps)
        assert isinstance(resultado, pd.DataFrame)


# ─────────────────────────────────────────────
# 4. TESTES: translate_weather_code
# ─────────────────────────────────────────────


class TestTranslateWeatherCode:
    def test_traducao_valida(self, df_achatado):
        # 61 é 'Chuva leve' no WMO_CODES
        df = translate_weather_code(df_achatado)
        assert "condicao_clima" in df.columns
        assert df["condicao_clima"].iloc[0] == "Chuva leve"
        assert "weather_code" not in df.columns

    def test_traducao_codigo_desconhecido(self, df_achatado):
        df = df_achatado.copy()
        df["weather_code"] = 999
        df = translate_weather_code(df)
        assert df["condicao_clima"].iloc[0] == "Desconhecido"


# ─────────────────────────────────────────────
# 5. TESTES: validate_rain_schema
# ─────────────────────────────────────────────


class TestValidateRainSchema:
    def test_cria_coluna_zerada_quando_ausente(self, df_achatado):
        # Garante que a coluna não existe
        df_sem_chuva = df_achatado.drop(columns=["chuva_1h_mm"], errors="ignore")

        resultado = validate_rain_schema(df_sem_chuva)

        assert "chuva_1h_mm" in resultado.columns
        assert resultado["chuva_1h_mm"].iloc[0] == 0.0

    def test_preenche_nan_com_zero_quando_coluna_existe(self, df_achatado):
        df_com_nan = df_achatado.copy()
        df_com_nan["chuva_1h_mm"] = float("nan")

        resultado = validate_rain_schema(df_com_nan)

        assert resultado["chuva_1h_mm"].iloc[0] == pytest.approx(0.0)
        assert not resultado["chuva_1h_mm"].isna().any()

    def test_preserva_valor_quando_chuva_real(self, df_achatado):
        resultado = validate_rain_schema(df_achatado)
        assert resultado["chuva_1h_mm"].iloc[0] == pytest.approx(0.37)


# ─────────────────────────────────────────────
# 6. TESTES: calculate_risk_level
# ─────────────────────────────────────────────


class TestCalculateRiskLevel:
    def _criar_df_risco(self, chuva: float, vento: float, umidade: int) -> pd.DataFrame:
        """Helper privado: cria um DataFrame mínimo para testar os limiares."""
        return pd.DataFrame(
            [
                {
                    "cidade": "Salvador",
                    "chuva_1h_mm": chuva,
                    "vento_velocidade_ms": vento,
                    "umidade_pct": umidade,
                    "temperatura_c": 28.0,
                }
            ]
        )

    def test_chuva_acima_50mm_e_critico(self):
        """LIMIAR: chuva >= 50mm → CRÍTICO."""
        df = self._criar_df_risco(chuva=55.0, vento=5.0, umidade=60)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_vento_acima_20ms_e_critico(self):
        """LIMIAR: vento >= 20 m/s → CRÍTICO."""
        df = self._criar_df_risco(chuva=0.0, vento=21.0, umidade=50)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_chuva_forte_e_vento_forte_e_critico(self):
        """LIMIAR: chuva >= 30mm E vento >= 15m/s combinados → CRÍTICO."""
        df = self._criar_df_risco(chuva=31.0, vento=16.0, umidade=80)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_chuva_acima_30mm_e_alerta(self):
        """LIMIAR: chuva >= 30mm (mas < 50mm e vento fraco) → ALERTA."""
        df = self._criar_df_risco(chuva=35.0, vento=5.0, umidade=70)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "ALERTA"

    def test_chuva_10mm_e_umidade_90pct_e_atencao(self):
        """LIMIAR: chuva >= 10mm E umidade >= 90% → ATENÇÃO."""
        df = self._criar_df_risco(chuva=11.0, vento=3.0, umidade=95)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "ATENÇÃO"

    def test_condicao_tranquila_e_normal(self):
        """LIMIAR: nenhuma condição crítica → NORMAL."""
        df = self._criar_df_risco(chuva=0.5, vento=3.0, umidade=60)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "NORMAL"

    def test_ordem_importa_critico_vence_alerta(self):
        """REGRA DE PRIORIDADE: crítico vence alerta."""
        df = self._criar_df_risco(chuva=55.0, vento=5.0, umidade=90)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_coluna_nivel_risco_criada(self):
        df = self._criar_df_risco(chuva=1.0, vento=2.0, umidade=50)
        resultado = calculate_risk_level(df)
        assert "nivel_risco" in resultado.columns

    def test_nao_modifica_dataframe_original(self):
        df = self._criar_df_risco(chuva=1.0, vento=2.0, umidade=50)
        calculate_risk_level(df)
        assert "nivel_risco" not in df.columns


# ─────────────────────────────────────────────
# 7. TESTES: load_from_bronze
# ─────────────────────────────────────────────


class TestLoadFromBronze:
    def test_retorna_dados_do_minio(self, raw_json_completo):
        with patch(
            "src.transform.download_from_bronze", return_value=raw_json_completo
        ):
            resultado = load_from_bronze(
                "weather_data/2026-04-30/10-00-00_salvador.json"
            )

        assert isinstance(resultado, dict)
        assert "current" in resultado

    def test_minio_falha_retorna_dict_vazio(self):
        with patch("src.transform.download_from_bronze", return_value={}):
            resultado = load_from_bronze("chave_invalida.json")

        assert resultado == {}


# ─────────────────────────────────────────────
# 8. TESTES: save_silver_data
# ─────────────────────────────────────────────


class TestSaveSilverData:
    def test_salva_arquivo_local(self, tmp_path, df_silver):
        destino = tmp_path / "weather_silver.json"

        with patch("src.transform.upload_to_silver", return_value=None):
            save_silver_data(df_silver, destino)

        assert destino.exists()

    def test_arquivo_local_tem_conteudo_json_valido(self, tmp_path, df_silver):
        import json

        destino = tmp_path / "weather_silver.json"

        with patch("src.transform.upload_to_silver", return_value=None):
            save_silver_data(df_silver, destino)

        conteudo = json.loads(destino.read_text(encoding="utf-8"))
        assert isinstance(conteudo, list)
        assert len(conteudo) == 1

    def test_upload_minio_chamado_quando_bronze_key_fornecida(
        self, tmp_path, df_silver
    ):
        destino = tmp_path / "weather_silver.json"
        bronze_key = "weather_data/2026-04-30/10-00-00_salvador.json"

        with patch(
            "src.transform.upload_to_silver", return_value="chave"
        ) as mock_silver:
            save_silver_data(df_silver, destino, bronze_key=bronze_key)

        mock_silver.assert_called_once()
        silver_key_usada = mock_silver.call_args[0][1]
        assert silver_key_usada.startswith("weather_silver/")

    def test_upload_minio_nao_chamado_sem_bronze_key(self, tmp_path, df_silver):
        destino = tmp_path / "weather_silver.json"

        with patch("src.transform.upload_to_silver") as mock_silver:
            save_silver_data(df_silver, destino)

        mock_silver.assert_not_called()


# ─────────────────────────────────────────────
# 9. TESTES: run_pipeline (Orquestrador)
# ─────────────────────────────────────────────


class TestRunPipeline:
    def test_pipeline_modo_local_retorna_dataframe(self, tmp_path, raw_json_completo):
        import json

        arquivo_entrada = tmp_path / "weather_data.json"
        arquivo_saida = tmp_path / "weather_silver.json"
        arquivo_entrada.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        with patch("src.transform.upload_to_silver", return_value=None):
            resultado = run_pipeline(
                input_path=arquivo_entrada,
                output_path=arquivo_saida,
            )

        assert isinstance(resultado, pd.DataFrame)
        assert "nivel_risco" in resultado.columns
        assert "cidade" in resultado.columns

    def test_pipeline_modo_local_cria_arquivo_silver(self, tmp_path, raw_json_completo):
        import json

        arquivo_entrada = tmp_path / "weather_data.json"
        arquivo_saida = tmp_path / "weather_silver.json"
        arquivo_entrada.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        with patch("src.transform.upload_to_silver", return_value=None):
            run_pipeline(input_path=arquivo_entrada, output_path=arquivo_saida)

        assert arquivo_saida.exists()

    def test_pipeline_modo_producao_usa_bronze_key(self, tmp_path, raw_json_completo):
        arquivo_saida = tmp_path / "weather_silver.json"
        bronze_key = "weather_data/2026-04-30/10-00-00_salvador.json"

        with patch(
            "src.transform.download_from_bronze", return_value=raw_json_completo
        ):
            with patch("src.transform.upload_to_silver", return_value=None):
                resultado = run_pipeline(
                    output_path=arquivo_saida,
                    bronze_key=bronze_key,
                )

        assert isinstance(resultado, pd.DataFrame)
        assert len(resultado) == 1

    def test_pipeline_bronze_key_invalida_lanca_value_error(self, tmp_path):
        arquivo_saida = tmp_path / "weather_silver.json"

        with patch("src.transform.download_from_bronze", return_value={}):
            with pytest.raises(ValueError, match="Falha ao carregar Bronze"):
                run_pipeline(
                    output_path=arquivo_saida,
                    bronze_key="chave_invalida.json",
                )
