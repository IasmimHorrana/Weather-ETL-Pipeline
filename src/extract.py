import requests
import json
import logging
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_weather_data(base_url: str) -> dict:
    """
    Faz a requisição à API, valida a resposta e persiste o JSON em disco.
    Retorna o dict com os dados ou {} em caso de falha.
    """
    logging.info(f"Iniciando extração de: {base_url}")

    # 1. Requisição com tratamento de erro de rede
    try:
        response = requests.get(base_url, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error(f"Falha na requisição: {e}")
        return {}

    # 2. Validação do status ANTES de parsear o corpo
    if response.status_code != 200:
        logging.error(f"Status inesperado: {response.status_code} — {response.text}")
        return {}

    # 3. Parse do JSON
    data = response.json()

    if not data:
        logging.error("Resposta vazia recebida da API")
        return {}

    # 4. Persistência em disco
    output_path = Path(__file__).parent.parent / 'data' / 'weather_data.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    # 5. Log útil — mostra a cidade extraída
    city = data.get('name', 'desconhecida')
    logging.info(f"Dados extraídos com sucesso para a cidade: {city}")

    return data