"""Abstração de leitura e escrita no MinIO (Bronze). Trocar por S3 real = só mudar .env."""

import functools
import json
import logging
import os
import typing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Carrega o .env centralizado do projeto (config/.env) antes de qualquer getenv.
# find_dotenv() sobe o diretório até encontrar o arquivo — funciona de qualquer CWD.
_env_path = Path(__file__).parent.parent / "config" / ".env"
load_dotenv(dotenv_path=_env_path)

logger = logging.getLogger(__name__)

# Credenciais lidas do .env — falha imediata (Fail Fast) se ausentes.

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY: str = os.getenv("MINIO_ROOT_PASSWORD", "")
BRONZE_BUCKET: str = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
SILVER_BUCKET: str = os.getenv("MINIO_SILVER_BUCKET", "silver")


@functools.lru_cache(maxsize=1)
def _get_s3_client() -> typing.Any:
    """Retorna um cliente boto3 configurado para o MinIO (s3v4). Singleton via cache."""
    variaveis_obrigatorias = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ACCESS_KEY,
        "MINIO_ROOT_PASSWORD": MINIO_SECRET_KEY,
        "MINIO_BRONZE_BUCKET": BRONZE_BUCKET,
        "MINIO_SILVER_BUCKET": SILVER_BUCKET,
    }
    ausentes = [nome for nome, valor in variaveis_obrigatorias.items() if not valor]
    if ausentes:
        raise OSError(
            f"Variáveis de ambiente obrigatórias não configuradas: {ausentes}\n"
            "Verifique se o arquivo config/.env está carregado corretamente."
        )

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO ignora a região, mas boto3 exige o campo
    )


def _ensure_bucket_exists(client: typing.Any, bucket_name: str) -> None:
    """Cria o bucket se não existir. Propaga outros erros."""
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.debug(f"Bucket '{bucket_name}' já existe.")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            # Outro erro (ex: permissão negada) → propaga para o chamador
            raise


def _build_bronze_key(city: str = "salvador") -> str:
    """Gera o object key particionado por data: weather_data/YYYY-MM-DD/HH-MM-SS_<cidade>.json"""
    now = datetime.now(tz=UTC)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    return f"weather_data/{date_str}/{time_str}_{city.lower()}.json"


def upload_to_bronze(data: dict, city: str = "salvador") -> str | None:
    """
    Persiste o JSON bruto no bucket Bronze.

    Retorna o object_key do arquivo salvo, ou None em caso de falha.
    """
    if not data:
        logger.error("Dados vazios recebidos — nada para salvar no Bronze.")
        return None

    client = _get_s3_client()

    try:
        _ensure_bucket_exists(client, BRONZE_BUCKET)

        object_key = _build_bronze_key(city)
        json_bytes = json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8")

        client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=object_key,
            Body=json_bytes,
            ContentType="application/json",
        )

        logger.info(f"[Bronze] Arquivo salvo: s3://{BRONZE_BUCKET}/{object_key}")
        return object_key

    except ClientError as e:
        logger.error(f"[Bronze] Falha ao salvar no MinIO: {e}")
        return None


def upload_to_silver(data_json: str, object_key: str) -> str | None:
    """
    Persiste o JSON processado no bucket Silver.

    Retorna o object_key ou None em caso de falha.
    """
    if not data_json:
        logger.error("Dados vazios recebidos — nada para salvar na Silver.")
        return None

    client = _get_s3_client()

    try:
        _ensure_bucket_exists(client, SILVER_BUCKET)

        client.put_object(
            Bucket=SILVER_BUCKET,
            Key=object_key,
            Body=data_json.encode("utf-8"),
            ContentType="application/json",
        )

        logger.info(f"[Silver] Arquivo salvo: s3://{SILVER_BUCKET}/{object_key}")
        return object_key

    except ClientError as e:
        logger.error(f"[Silver] Falha ao salvar no MinIO (Silver): {e}")
        return None


def download_from_bronze(object_key: str) -> dict[str, object]:
    """Baixa e desserializa um JSON do Bronze. Retorna {} em caso de falha."""
    if not object_key:
        logger.error("[Bronze] object_key não informado.")
        return {}

    client = _get_s3_client()

    try:
        response = client.get_object(Bucket=BRONZE_BUCKET, Key=object_key)
        raw_bytes = response["Body"].read()
        data: dict[str, object] = json.loads(raw_bytes.decode("utf-8"))

        logger.info(f"[Bronze] Arquivo carregado: s3://{BRONZE_BUCKET}/{object_key}")
        return data

    except ClientError as e:
        logger.error(f"[Bronze] Falha ao baixar do MinIO: {e}")
        return {}


def download_from_silver(object_key: str) -> str:
    """Baixa o JSON da camada Silver e o retorna como string. Retorna string vazia em caso de falha."""
    if not object_key:
        logger.error("[Silver] object_key não informado.")
        return ""

    client = _get_s3_client()

    try:
        response = client.get_object(Bucket=SILVER_BUCKET, Key=object_key)
        raw_bytes = response["Body"].read()
        logger.info(f"[Silver] Arquivo carregado: s3://{SILVER_BUCKET}/{object_key}")
        return str(raw_bytes.decode("utf-8"))

    except ClientError as e:
        logger.error(f"[Silver] Falha ao baixar do MinIO: {e}")
        return ""


def list_bronze_files(prefix: str = "weather_data/") -> list[str]:
    """Lista object keys no Bronze com o prefixo dado. Retorna [] em caso de falha."""
    client = _get_s3_client()

    try:
        paginator = client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
            if "Contents" in page:
                files.extend([obj["Key"] for obj in page["Contents"]])

        logger.info(
            f"[Bronze] {len(files)} arquivo(s) encontrado(s) com prefixo '{prefix}'."
        )
        return files

    except ClientError as e:
        logger.error(f"[Bronze] Falha ao listar arquivos: {e}")
        return []


def list_silver_files(prefix: str = "weather_silver/") -> list[str]:
    """Lista object keys no Silver com o prefixo dado. Retorna [] em caso de falha."""
    client = _get_s3_client()

    try:
        paginator = client.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=prefix):
            if "Contents" in page:
                files.extend([obj["Key"] for obj in page["Contents"]])

        logger.info(
            f"[Silver] {len(files)} arquivo(s) encontrado(s) com prefixo '{prefix}'."
        )
        return files

    except ClientError as e:
        logger.error(f"[Silver] Falha ao listar arquivos: {e}")
        return []


# --- Teste Manual ---
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Simula um dado bruto da API
    dados_teste = {
        "name": "Salvador",
        "main": {"temp": 28.5, "humidity": 82},
        "wind": {"speed": 7.2},
        "rain": {"1h": 0.5},
    }

    print("\n--- Teste: upload_to_bronze ---")
    key = upload_to_bronze(dados_teste, city="salvador")
    print(f"Arquivo salvo em: {key}")

    if key:
        print("\n--- Teste: download_from_bronze ---")
        recuperado = download_from_bronze(key)
        main_info = cast(dict, recuperado.get("main", {}))
        print(
            f"Dado recuperado: {recuperado.get('name')} | Temp: {main_info.get('temp')}°C"
        )

        print("\n--- Teste: list_bronze_files ---")
        arquivos = list_bronze_files()
        for f in arquivos:
            print(f"  → {f}")
