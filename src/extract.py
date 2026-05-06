"""Extrai dados da Open-Meteo API e persiste o JSON bruto no MinIO (Bronze)."""

import json
import logging
from pathlib import Path

import requests

from src.storage import upload_to_bronze

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Open-Meteo não exige API Key.
# Campos escolhidos: temperatura, umidade, chuva, vento e código de condição (WMO).
OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=-12.9711"
    "&longitude=-38.5108"
    "&current=temperature_2m,relative_humidity_2m,rain,weather_code,wind_speed_10m"
    "&timezone=America/Bahia"
    "&timeformat=unixtime"
)

# Cidade fixada porque a Open-Meteo não retorna nome — só coordenadas.
CITY = "Salvador"


def extract_weather_data(base_url: str) -> tuple[dict, str | None]:
    """Requisita a API e persiste no MinIO. Retorna (dados, object_key)."""
    logging.info(f"Iniciando extração de: {base_url}")

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"Erro HTTP: {e} — {e.response.text}")
        return {}, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Falha na requisição: {e}")
        return {}, None

    data = response.json()

    if not data or "current" not in data:
        logging.error("Resposta inválida ou vazia recebida da Open-Meteo.")
        return {}, None

    logging.info(f"Dados extraídos com sucesso para: {CITY}")

    object_key = upload_to_bronze(data, city=CITY)

    if object_key:
        logging.info(f"[✔] Extração concluída. Bronze key: {object_key}")
    else:
        logging.warning("[✘] Upload para o MinIO falhou.")

    return data, object_key


def _save_local_fallback(data: dict, path: Path) -> None:
    """Salva o JSON bruto em disco local como fallback de desenvolvimento."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    logging.info(f"Fallback local salvo em: {path}")


if __name__ == "__main__":
    data, key = extract_weather_data(OPEN_METEO_URL)

    if data:
        fallback_path = Path(__file__).parent.parent / "data" / "weather_data.json"
        _save_local_fallback(data, fallback_path)

    if key:
        print(f"\n✅ Bronze key: {key}")
        print(f"   Verifique em: http://localhost:9001 → weather-bronze → {key}")
    else:
        print("\n❌ Falha na coleta. Verifique os logs acima.")