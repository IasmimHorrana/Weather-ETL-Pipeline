"""Extrai dados da OpenWeather API e persiste o JSON bruto no MinIO (Bronze)."""

import json
import logging
from pathlib import Path

import requests

from src.storage import upload_to_bronze

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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

    if not data:
        logging.error("Resposta vazia recebida da API")
        return {}, None

    city = data.get("name", "desconhecida")
    logging.info(f"Dados extraídos com sucesso para: {city}")

    object_key = upload_to_bronze(data, city=city)

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
    import os

    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).parent.parent / "config" / ".env")

    api_key = os.getenv("API_KEY", "")
    if not api_key:
        raise OSError("API_KEY não encontrada. Configure no config/.env")

    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?q=Salvador,BR&appid={api_key}&units=metric&lang=pt_br"
    )

    data, key = extract_weather_data(url)

    # Fallback local — salva em disco independente do MinIO
    if data:
        fallback_path = Path(__file__).parent.parent / "data" / "weather_data.json"
        _save_local_fallback(data, fallback_path)

    if key:
        print(f"\n✅ Bronze key: {key}")
        print(f"   Verifique em: http://localhost:9001 → weather-bronze → {key}")
    else:
        print("\n❌ Falha na coleta. Verifique os logs acima.")
