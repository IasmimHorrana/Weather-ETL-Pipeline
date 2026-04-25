"""
extract.py — Camada de Extração (Extract) do Pipeline ETL.

Responsabilidade única: fazer a requisição na OpenWeather API,
validar a resposta e entregar o dado bruto para a camada Bronze (MinIO).

Mudança arquitetural (Bronze Layer):
    Antes: salvava o JSON em disco local (data/weather_data.json).
    Agora: faz upload para o MinIO via storage.upload_to_bronze().

    O arquivo local em disco foi mantido como FALLBACK de desenvolvimento
    (para rodar o transform.py sem precisar do MinIO no ar).
    Em produção (via Airflow), o fluxo usa exclusivamente o MinIO.

Retorno:
    A função retorna o object_key do arquivo salvo no MinIO.
    Isso permite que o Airflow passe o caminho exato para o transform.py
    sem precisar adivinhar qual arquivo processar.
"""

import requests
import json
import logging
from pathlib import Path

from src.storage import upload_to_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_weather_data(base_url: str) -> str | None:
    """
    Faz a requisição à API, valida a resposta e persiste o JSON bruto no MinIO (Bronze).

    Parâmetros:
        base_url: URL completa da OpenWeather API (com cidade e chave)

    Retorna:
        object_key do arquivo no MinIO (ex: 'weather_data/2026-04-25/14-30-00_salvador.json')
        Retorna None se a extração ou o upload falharem.
    """
    logging.info(f"Iniciando extração de: {base_url}")

    # 1. Requisição com tratamento de erro de rede
    try:
        response = requests.get(base_url, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Falha na requisição: {e}")
        return None

    # 2. Validação do status ANTES de parsear o corpo
    if response.status_code != 200:
        logging.error(f"Status inesperado: {response.status_code} — {response.text}")
        return None

    # 3. Parse do JSON
    data = response.json()

    if not data:
        logging.error("Resposta vazia recebida da API")
        return None

    # 4. Log útil — mostra a cidade extraída
    city = data.get("name", "desconhecida")
    logging.info(f"Dados extraídos com sucesso para: {city}")

    # 5. Salva também em disco como fallback de desenvolvimento
    # Mantido para compatibilidade com o transform.py em modo local.
    # Em produção com Airflow, o transform.py lê diretamente do MinIO.
    output_path = Path(__file__).parent.parent / "data" / "weather_data.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    logging.info(f"Fallback local salvo em: {output_path}")

    # 6. Upload para o MinIO (Bronze) — principal destino em produção
    object_key = upload_to_bronze(data, city=city)

    if object_key:
        logging.info(f"[✔] Extração concluída. Bronze key: {object_key}")
    else:
        logging.warning("Upload para o MinIO falhou. Dado disponível apenas no fallback local.")

    return object_key