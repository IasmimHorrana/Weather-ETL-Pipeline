import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.storage import download_from_bronze, upload_to_silver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DEFAULT_DATA_PATH = Path(__file__).parent.parent / "data" / "weather_data.json"

REGRAS_RISCO = {
    "chuva_critica_mm": 50.0,
    "vento_critico_ms": 20.0,
    "chuva_alerta_mm": 30.0,
    "vento_alerta_ms": 15.0,
    "chuva_atencao_mm": 10.0,
    "umidade_atencao_pct": 90.0,
}

# Tradução dos códigos WMO da Open-Meteo para descrição legível
WMO_CODES: dict[int, str] = {
    0: "Céu limpo",
    1: "Principalmente limpo",
    2: "Parcialmente nublado",
    3: "Nublado",
    45: "Neblina",
    48: "Neblina com geada",
    51: "Garoa leve",
    53: "Garoa moderada",
    55: "Garoa intensa",
    61: "Chuva leve",
    63: "Chuva moderada",
    65: "Chuva forte",
    80: "Pancadas de chuva leve",
    81: "Pancadas de chuva moderada",
    82: "Pancadas de chuva forte",
    95: "Tempestade",
    96: "Tempestade com granizo leve",
    99: "Tempestade com granizo intenso",
}


# 1a. Função de Extração a partir do MinIO (Bronze)
def load_from_bronze(object_key: str) -> dict:
    logging.info(f"[Bronze] Carregando dado: {object_key}")
    data = download_from_bronze(object_key)
    if not data:
        logging.error(f"[Bronze] Dado vazio ou falha no download: {object_key}")
    return data


# 1b. Função de Extração a partir do disco — modo desenvolvimento (fallback)
def load_raw_json(path_name: str | Path) -> dict[str, object]:
    logging.info(f"Lendo JSON do caminho local: {path_name}")
    path = Path(path_name)
    if not path.exists():
        logging.error(f"Arquivo não encontrado: {path}")
        raise FileNotFoundError(f"Path {path} does not exist")
    with open(path) as f:
        data: dict[str, object] = json.load(f)
    logging.info("JSON carregado na memória com sucesso.")
    return data


# 2. Função de Flattening — adaptada para Open-Meteo
def flatten_to_dataframe(raw_data: dict) -> pd.DataFrame:
    """
    Extrai os campos do bloco 'current' e adiciona lat/lon do nível raiz.
    O JSON da Open-Meteo é plano — não precisa de json_normalize com record_path.
    """
    logging.info("Iniciando o achatamento (flattening) dos dados Open-Meteo.")

    current = raw_data.get("current", {})

    if not current:
        raise ValueError("Campo 'current' ausente no JSON da Open-Meteo.")

    row = {
        "cidade": "Salvador",  # Open-Meteo não retorna nome
        "pais": "BR",
        "latitude": raw_data.get("latitude"),
        "longitude": raw_data.get("longitude"),
        "data_hora": current.get("time"),
        "temperatura_c": current.get("temperature_2m"),
        "umidade_pct": current.get("relative_humidity_2m"),
        "chuva_1h_mm": current.get("rain", 0.0),
        "weather_code": current.get("weather_code"),
        "vento_velocidade_ms": round(current.get("wind_speed_10m", 0.0) / 3.6, 2),
    }

    df = pd.DataFrame([row])
    logging.info("Achatamento concluído. Tabela criada.")
    return df


# 3. Função de Tratamento de Datas e Fuso
def convert_timestamps_to_local(
    df: pd.DataFrame, timezone: str = "America/Bahia"
) -> pd.DataFrame:
    """Converte Unix Timestamp de 'data_hora' para horário local."""
    logging.info(f"Convertendo timestamps para o fuso horário: {timezone}")
    df = df.copy()

    if "data_hora" in df.columns:
        df["data_hora"] = (
            pd.to_datetime(df["data_hora"], unit="s")
            .dt.tz_localize("UTC")
            .dt.tz_convert(timezone)
        )

    logging.info("Conversão de datas finalizada com sucesso.")
    return df


# 4. Função de Tradução do Código WMO → condicao_clima
def translate_weather_code(df: pd.DataFrame) -> pd.DataFrame:
    """Traduz o código WMO numérico para descrição legível em português."""
    logging.info("Traduzindo weather_code para condicao_clima.")
    df = df.copy()

    if "weather_code" in df.columns:
        df["condicao_clima"] = df["weather_code"].map(WMO_CODES).fillna("Desconhecido")
        df = df.drop(columns=["weather_code"])

    return df


# 5. Função de Validação de Schema (Tratamento de Chuva)
def validate_rain_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que chuva_1h_mm exista e preenche nulos com 0.0."""
    logging.info("Validando schema para a coluna de chuva.")
    df = df.copy()
    df["chuva_1h_mm"] = df.get("chuva_1h_mm", pd.Series([0.0])).fillna(0.0)
    logging.info("Validação de chuva finalizada.")
    return df


# 6. Função de Enriquecimento de Dados (Regra de Negócio)
def calculate_risk_level(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula o nível de risco com base em condições climáticas."""
    logging.info("Calculando a coluna de Nível de Risco.")
    df = df.copy()

    chuva = df["chuva_1h_mm"]
    vento = df["vento_velocidade_ms"]
    umidade = df["umidade_pct"]

    condicoes = [
        (chuva >= REGRAS_RISCO["chuva_critica_mm"])
        | (vento >= REGRAS_RISCO["vento_critico_ms"])
        | (
            (chuva >= REGRAS_RISCO["chuva_alerta_mm"])
            & (vento >= REGRAS_RISCO["vento_alerta_ms"])
        ),
        (chuva >= REGRAS_RISCO["chuva_alerta_mm"]),
        (chuva >= REGRAS_RISCO["chuva_atencao_mm"])
        & (umidade >= REGRAS_RISCO["umidade_atencao_pct"]),
    ]
    valores = ["CRÍTICO", "ALERTA", "ATENÇÃO"]

    df["nivel_risco"] = np.select(condicoes, valores, default="NORMAL")
    logging.info("Nível de risco calculado com sucesso.")
    return df


# 7. Função de Exportação (Load / Camada Silver)
def save_silver_data(
    df: pd.DataFrame, output_path: str | Path, bronze_key: str | None = None
) -> None:
    logging.info("Iniciando a exportação dos dados (Silver).")
    json_data = df.to_json(orient="records", indent=4, date_format="iso")

    if bronze_key:
        silver_key = bronze_key.replace("weather_data/", "weather_silver/", 1)
        upload_to_silver(json_data, silver_key)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json_data)

    logging.info(f"Dados exportados! Shape final: {df.shape}")
    print("\n[✔] Pipeline Concluído!")
    print(f"Shape salvo: {df.shape}")
    print("\nTipos de Dados:")
    print(df.dtypes)


# --- ORQUESTRADOR DA ESTEIRA ---
def run_pipeline(
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    bronze_key: str | None = None,
) -> pd.DataFrame:
    logging.info("--- INICIANDO PIPELINE DE DADOS ---")

    if bronze_key:
        logging.info(f"Modo Produção — MinIO: {bronze_key}")
        dicionario_cru = load_from_bronze(bronze_key)
        if not dicionario_cru:
            raise ValueError(f"Falha ao carregar Bronze: {bronze_key}")
    else:
        logging.info("Modo Local — lendo do disco.")
        input_path = input_path or DEFAULT_DATA_PATH
        dicionario_cru = load_raw_json(input_path)

    df_achatado = flatten_to_dataframe(dicionario_cru)
    df_com_datas = convert_timestamps_to_local(df_achatado)
    df_com_codigo = translate_weather_code(df_com_datas)  # etapa nova
    df_com_chuva = validate_rain_schema(df_com_codigo)
    df_enriquecido = calculate_risk_level(df_com_chuva)

    output_path = output_path or (DEFAULT_DATA_PATH.parent / "weather_silver.json")
    save_silver_data(df_enriquecido, output_path, bronze_key)

    return df_enriquecido


# --- ÁREA DE EXECUÇÃO ---
if __name__ == "__main__":
    from src.storage import list_bronze_files

    arquivos_bronze = list_bronze_files()
    bronze_key_recente = arquivos_bronze[-1] if arquivos_bronze else None

    arquivo_saida = Path(__file__).parent.parent / "data" / "weather_silver.json"

    if bronze_key_recente:
        print(f"\n[Testando integração] MinIO: {bronze_key_recente}")
    else:
        print("\n[Modo Local] Rodando com fallback local.")

    df_final = run_pipeline(
        DEFAULT_DATA_PATH, arquivo_saida, bronze_key=bronze_key_recente
    )
