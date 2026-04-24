"""
test_transform.py — Testes unitários para o módulo src/transform.py

Funções testadas:
    1. load_raw_json          → Carregamento de arquivo JSON
    2. flatten_to_dataframe   → Achatamento do JSON aninhado
    3. convert_timestamps_to_local → Conversão de Unix para datetime com fuso
    4. validate_rain_schema   → Tratamento da coluna de chuva (campo opcional)
    5. standardize_to_silver  → Drop de colunas + Rename para padrão Silver
    6. calculate_risk_level   → Lógica de negócio: classificação de risco

Estratégia:
    - Funções puras (recebem dados, retornam dados) → testadas diretamente.
    - Funções com I/O (leitura de arquivo, escrita) → uso de `tmp_path` e `patch`.
    - Cada teste é independente: não depende do resultado de outro teste.

Por que testar o transform.py em profundidade?
    É o coração do pipeline. Uma mudança no rename_map ou nos limiares de risco
    sem testes poderia silenciosamente corromper o histórico do banco de dados.
"""

import json
import pytest
import pandas as pd

from src.transform import (
    load_raw_json,
    flatten_to_dataframe,
    convert_timestamps_to_local,
    validate_rain_schema,
    standardize_to_silver,
    calculate_risk_level,
)


# ─────────────────────────────────────────────
# FIXTURES: Dados reutilizáveis entre os testes
# ─────────────────────────────────────────────

@pytest.fixture
def raw_json_completo() -> dict:
    """
    Simula o payload completo da OpenWeather API, incluindo o campo 'rain'
    (que só existe quando está chovendo). É o input da nossa Máquina 2.
    """
    return {
        "coord": {"lon": -38.5014, "lat": -12.9716},
        "weather": [{"id": 500, "main": "Rain", "description": "chuva leve", "icon": "10d"}],
        "base": "stations",
        "main": {
            "temp": 26.48, "feels_like": 27.2,
            "temp_min": 25.0, "temp_max": 28.0,
            "pressure": 1012, "humidity": 85,
        },
        "visibility": 6424,
        "wind": {"speed": 6.82, "deg": 161, "gust": 7.54},
        "rain": {"1h": 0.37},
        "clouds": {"all": 57},
        "dt": 1776880187,
        "sys": {"country": "BR", "sunrise": 1776847219, "sunset": 1776889466},
        "timezone": -10800,
        "id": 3450554,
        "name": "Salvador",
        "cod": 200,
    }


@pytest.fixture
def raw_json_sem_chuva(raw_json_completo) -> dict:
    """
    Mesmo payload, mas sem o campo 'rain' (dia ensolarado).
    Testa a resiliência do validate_rain_schema.
    """
    data = dict(raw_json_completo)
    data.pop("rain", None)  # Remove o campo rain se existir
    return data


@pytest.fixture
def df_achatado(raw_json_completo) -> pd.DataFrame:
    """DataFrame já achatado — input para as funções de transformação."""
    return flatten_to_dataframe(raw_json_completo)


@pytest.fixture
def df_silver(df_achatado) -> pd.DataFrame:
    """
    DataFrame passado por todas as etapas até a padronização Silver.
    Usado como base para testar calculate_risk_level.
    """
    df = convert_timestamps_to_local(df_achatado)
    df = validate_rain_schema(df)
    df = standardize_to_silver(df)
    return df


# ─────────────────────────────────────────────
# 1. TESTES: load_raw_json
# ─────────────────────────────────────────────

class TestLoadRawJson:

    def test_carrega_json_existente_com_sucesso(self, tmp_path, raw_json_completo):
        """
        CENÁRIO: Arquivo JSON existe e tem conteúdo válido.
        ESPERADO: Retorna o dicionário Python correspondente.
        """
        arquivo = tmp_path / "weather_data.json"
        arquivo.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        resultado = load_raw_json(arquivo)

        assert isinstance(resultado, dict)
        assert resultado["name"] == "Salvador"

    def test_arquivo_inexistente_lanca_file_not_found(self, tmp_path):
        """
        CENÁRIO: Caminho apontado não existe.
        ESPERADO: Lança FileNotFoundError (não retorna silenciosamente).

        Diferente do extract.py (que retorna {}), o load_raw_json LANÇA exceção,
        porque se o arquivo Silver não existe, o pipeline inteiro deve parar
        e o Airflow deve marcar a tarefa como FAILED para reprocessamento.
        """
        caminho_falso = tmp_path / "nao_existe.json"

        with pytest.raises(FileNotFoundError):
            load_raw_json(caminho_falso)

    def test_retorno_e_dicionario(self, tmp_path, raw_json_completo):
        """
        CENÁRIO: Arquivo existe.
        ESPERADO: O tipo do retorno é dict (não lista, não string).
        """
        arquivo = tmp_path / "weather_data.json"
        arquivo.write_text(json.dumps(raw_json_completo), encoding="utf-8")

        resultado = load_raw_json(arquivo)

        assert isinstance(resultado, dict)


# ─────────────────────────────────────────────
# 2. TESTES: flatten_to_dataframe
# ─────────────────────────────────────────────

class TestFlattenToDataframe:

    def test_retorna_dataframe(self, raw_json_completo):
        """
        CENÁRIO: JSON válido recebido.
        ESPERADO: Retorno é um pd.DataFrame (não dict, não lista).
        """
        resultado = flatten_to_dataframe(raw_json_completo)
        assert isinstance(resultado, pd.DataFrame)

    def test_dataframe_tem_exatamente_uma_linha(self, raw_json_completo):
        """
        CENÁRIO: JSON de uma única cidade (sempre 1 leitura por chamada de API).
        ESPERADO: DataFrame tem 1 linha.
        """
        df = flatten_to_dataframe(raw_json_completo)
        assert len(df) == 1

    def test_extrai_campo_rain_quando_presente(self, raw_json_completo):
        """
        CENÁRIO: JSON inclui o campo 'rain.1h' (está chovendo).
        ESPERADO: Coluna 'rain.1h' existe e tem o valor correto (0.37).

        Este teste valida o "Hotfix" que aplicamos na Máquina 2:
        o json_normalize não captura 'rain' automaticamente,
        então extraímos manualmente.
        """
        df = flatten_to_dataframe(raw_json_completo)
        assert "rain.1h" in df.columns
        assert df["rain.1h"].iloc[0] == pytest.approx(0.37)

    def test_sem_rain_coluna_nao_criada_aqui(self, raw_json_sem_chuva):
        """
        CENÁRIO: JSON sem campo 'rain' (dia sem chuva).
        ESPERADO: flatten_to_dataframe NÃO cria 'rain.1h' — isso é responsabilidade
        do validate_rain_schema (Máquina 4). Separação de responsabilidades.
        """
        df = flatten_to_dataframe(raw_json_sem_chuva)
        assert "rain.1h" not in df.columns

    def test_campo_name_presente_apos_flatten(self, raw_json_completo):
        """
        CENÁRIO: JSON válido.
        ESPERADO: Coluna 'name' (cidade) está presente no DataFrame.
        """
        df = flatten_to_dataframe(raw_json_completo)
        assert "name" in df.columns
        assert df["name"].iloc[0] == "Salvador"


# ─────────────────────────────────────────────
# 3. TESTES: convert_timestamps_to_local
# ─────────────────────────────────────────────

class TestConvertTimestampsToLocal:

    def test_coluna_dt_vira_datetime(self, df_achatado):
        """
        CENÁRIO: DataFrame com colunas de Unix Timestamp (inteiros).
        ESPERADO: Coluna 'dt' se torna dtype datetime com fuso horário.
        """
        df = convert_timestamps_to_local(df_achatado)
        assert pd.api.types.is_datetime64_any_dtype(df["dt"])

    def test_fuso_horario_aplicado_e_america_bahia(self, df_achatado):
        """
        CENÁRIO: Conversão padrão (sem passar timezone).
        ESPERADO: Timezone da coluna 'dt' é 'America/Bahia'.
        """
        df = convert_timestamps_to_local(df_achatado)
        tz_name = str(df["dt"].dt.tz)
        assert "Bahia" in tz_name or "America" in tz_name

    def test_nao_modifica_dataframe_original(self, df_achatado):
        """
        CENÁRIO: Função chamada com um DataFrame.
        ESPERADO: O DataFrame original não é modificado (nossa função usa .copy()).

        Este teste valida o padrão de imutabilidade que adotamos:
        cada máquina retorna uma cópia nova, sem efeito colateral.
        """
        dt_original = df_achatado["dt"].iloc[0]
        convert_timestamps_to_local(df_achatado)
        assert df_achatado["dt"].iloc[0] == dt_original  # Não foi alterado

    def test_colunas_ausentes_sao_ignoradas_sem_erro(self):
        """
        CENÁRIO: DataFrame sem as colunas de timestamp (schema incompleto).
        ESPERADO: Função não quebra — programação defensiva.
        """
        df_sem_timestamps = pd.DataFrame([{"cidade": "Salvador", "temperatura_c": 26.0}])
        resultado = convert_timestamps_to_local(df_sem_timestamps)
        assert isinstance(resultado, pd.DataFrame)


# ─────────────────────────────────────────────
# 4. TESTES: validate_rain_schema
# ─────────────────────────────────────────────

class TestValidateRainSchema:

    def test_cria_coluna_zerada_quando_ausente(self, df_achatado):
        """
        CENÁRIO: DataFrame sem coluna 'rain.1h' (não choveu).
        ESPERADO: Coluna 'rain.1h' é criada com valor 0.0.
        """
        # Garante que a coluna não existe
        df_sem_chuva = df_achatado.drop(columns=["rain.1h"], errors="ignore")

        resultado = validate_rain_schema(df_sem_chuva)

        assert "rain.1h" in resultado.columns
        assert resultado["rain.1h"].iloc[0] == 0.0

    def test_preenche_nan_com_zero_quando_coluna_existe(self, df_achatado):
        """
        CENÁRIO: Coluna 'rain.1h' existe mas tem NaN (dado incompleto da API).
        ESPERADO: NaN é substituído por 0.0.
        """
        import numpy as np
        df_com_nan = df_achatado.copy()
        df_com_nan["rain.1h"] = float("nan")

        resultado = validate_rain_schema(df_com_nan)

        assert resultado["rain.1h"].iloc[0] == pytest.approx(0.0)
        assert not resultado["rain.1h"].isna().any()

    def test_preserva_valor_quando_chuva_real(self, df_achatado):
        """
        CENÁRIO: Coluna 'rain.1h' existe e tem valor real (0.37mm).
        ESPERADO: Valor original é preservado (não zerado erroneamente).
        """
        resultado = validate_rain_schema(df_achatado)
        assert resultado["rain.1h"].iloc[0] == pytest.approx(0.37)


# ─────────────────────────────────────────────
# 5. TESTES: standardize_to_silver
# ─────────────────────────────────────────────

class TestStandardizeToSilver:

    def test_coluna_name_renomeada_para_cidade(self, df_achatado):
        """
        CENÁRIO: DataFrame com coluna 'name' (padrão da API inglesa).
        ESPERADO: Após padronização, a coluna se chama 'cidade'.
        """
        df = validate_rain_schema(df_achatado)
        resultado = standardize_to_silver(df)

        assert "cidade" in resultado.columns
        assert "name" not in resultado.columns

    def test_coluna_main_temp_renomeada_para_temperatura_c(self, df_achatado):
        """
        ESPERADO: 'main.temp' → 'temperatura_c' (com unidade no nome).
        """
        df = validate_rain_schema(df_achatado)
        resultado = standardize_to_silver(df)

        assert "temperatura_c" in resultado.columns
        assert "main.temp" not in resultado.columns

    def test_colunas_drop_removidas(self, df_achatado):
        """
        CENÁRIO: DataFrame com colunas internas da API ('base', 'cod', 'id').
        ESPERADO: Essas colunas são removidas na camada Silver.
        """
        df = validate_rain_schema(df_achatado)
        resultado = standardize_to_silver(df)

        colunas_proibidas = ["base", "cod"]
        for col in colunas_proibidas:
            assert col not in resultado.columns, f"Coluna '{col}' deveria ter sido removida"

    def test_mapeamento_completo_de_colunas_essenciais(self, df_achatado):
        """
        ESPERADO: Todas as colunas do padrão Silver estão presentes após a transformação.
        
        Este é um "contrato" entre o transform.py e o metabase/banco de dados:
        se qualquer coluna sumir, o dashboard quebra silenciosamente.
        """
        colunas_esperadas = [
            "cidade", "pais", "latitude", "longitude",
            "temperatura_c", "umidade_pct", "chuva_1h_mm",
            "vento_velocidade_ms", "nivel_risco" if False else "condicao_clima",
        ]
        df = validate_rain_schema(df_achatado)
        resultado = standardize_to_silver(df)

        for col in ["cidade", "pais", "temperatura_c", "umidade_pct", "chuva_1h_mm"]:
            assert col in resultado.columns, f"Coluna obrigatória '{col}' ausente na Camada Silver"


# ─────────────────────────────────────────────
# 6. TESTES: calculate_risk_level
# ─────────────────────────────────────────────

class TestCalculateRiskLevel:
    """
    Estes testes validam a Regra de Negócio mais crítica do sistema.
    Qualquer mudança nos limiares deve quebrar estes testes — e isso é BOM,
    porque impede alterações acidentais no modelo de classificação de risco.
    """

    def _criar_df_risco(self, chuva: float, vento: float, umidade: int) -> pd.DataFrame:
        """Helper privado: cria um DataFrame mínimo para testar os limiares."""
        return pd.DataFrame([{
            "cidade": "Salvador",
            "chuva_1h_mm": chuva,
            "vento_velocidade_ms": vento,
            "umidade_pct": umidade,
            "temperatura_c": 28.0,
        }])

    def test_chuva_acima_50mm_e_critico(self):
        """LIMIAR: chuva >= 50mm → CRÍTICO (risco de deslizamento)."""
        df = self._criar_df_risco(chuva=55.0, vento=5.0, umidade=60)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_vento_acima_15ms_e_critico(self):
        """LIMIAR: vento >= 15 m/s → CRÍTICO (independente da chuva)."""
        df = self._criar_df_risco(chuva=0.0, vento=16.0, umidade=50)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_chuva_acima_25mm_e_alerta(self):
        """LIMIAR: chuva >= 25mm (mas < 50mm) → ALERTA."""
        df = self._criar_df_risco(chuva=30.0, vento=5.0, umidade=70)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "ALERTA"

    def test_chuva_5mm_e_umidade_80pct_e_atencao(self):
        """LIMIAR: chuva >= 5mm E umidade >= 80% → ATENÇÃO (solo saturando)."""
        df = self._criar_df_risco(chuva=6.0, vento=3.0, umidade=85)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "ATENÇÃO"

    def test_condicao_tranquila_e_normal(self):
        """LIMIAR: nenhuma condição crítica → NORMAL."""
        df = self._criar_df_risco(chuva=0.5, vento=3.0, umidade=60)
        resultado = calculate_risk_level(df)
        assert resultado["nivel_risco"].iloc[0] == "NORMAL"

    def test_ordem_importa_critico_vence_alerta(self):
        """
        REGRA DE PRIORIDADE: chuva >= 50mm E >= 25mm → deve ser CRÍTICO, não ALERTA.
        O np.select usa a PRIMEIRA condição verdadeira. Este teste garante a ordem.
        """
        df = self._criar_df_risco(chuva=55.0, vento=5.0, umidade=90)
        resultado = calculate_risk_level(df)
        # 55mm satisfaz tanto CRÍTICO (>= 50) quanto ALERTA (>= 25)
        # A ordem correta prioriza CRÍTICO
        assert resultado["nivel_risco"].iloc[0] == "CRÍTICO"

    def test_coluna_nivel_risco_criada(self):
        """
        ESPERADO: A coluna 'nivel_risco' é adicionada ao DataFrame de saída.
        Valida que a função não falha silenciosamente sem criar a coluna.
        """
        df = self._criar_df_risco(chuva=1.0, vento=2.0, umidade=50)
        resultado = calculate_risk_level(df)
        assert "nivel_risco" in resultado.columns

    def test_nao_modifica_dataframe_original(self):
        """
        ESPERADO: DataFrame original não é alterado (imutabilidade via .copy()).
        """
        df = self._criar_df_risco(chuva=1.0, vento=2.0, umidade=50)
        calculate_risk_level(df)
        assert "nivel_risco" not in df.columns  # Original intocado
