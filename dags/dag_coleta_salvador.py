import io
import logging
import os
import typing
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================
# Configuração padrão compartilhada pelas tasks
# =============================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# =============================================================
# TASK 1 — Extração: OpenWeather API → MinIO (Bronze)
# =============================================================
def task_extract(**context: typing.Any) -> None:
    """
    Chama extract_weather_data() e empurra o bronze_key via XCom
    para que a task seguinte saiba qual arquivo processar.
    """
    from src.extract import extract_weather_data

    api_key = os.getenv("API_KEY", "")
    city = os.getenv("OPENWEATHER_CITY", "Salvador")
    openweather_url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={city},BR&appid={api_key}&units=metric&lang=pt_br"
    )

    _, bronze_key = extract_weather_data(base_url=openweather_url)

    if not bronze_key:
        raise RuntimeError("Extração falhou: upload para o MinIO retornou None.")

    logging.info(f"[extract] Bronze key gerada: {bronze_key}")
    context["ti"].xcom_push(key="bronze_key", value=bronze_key)


# =============================================================
# TASK 2 — Transformação: MinIO Bronze → MinIO Silver
# =============================================================
def task_transform(**context: typing.Any) -> None:
    """
    Lê o bronze_key do XCom, chama run_pipeline() e empurra o silver_key.
    run_pipeline() já cuida de normalizar, calcular risco e salvar na Silver.
    """
    from src.transform import run_pipeline

    bronze_key: str = context["ti"].xcom_pull(
        task_ids="extract_bronze", key="bronze_key"
    )

    if not bronze_key:
        raise ValueError("bronze_key não encontrada no XCom.")

    # silver_key espelha o bronze_key mas no prefixo weather_silver/
    silver_key = bronze_key.replace("weather_data/", "weather_silver/", 1)

    run_pipeline(bronze_key=bronze_key)

    logging.info(f"[transform] Silver key: {silver_key}")
    context["ti"].xcom_push(key="silver_key", value=silver_key)


# =============================================================
# TASK 3 — Carga histórica: MinIO Silver → PostgreSQL
# =============================================================
def task_load(**context: typing.Any) -> None:
    """
    Lê o silver_key do XCom e faz a inserção idempotente no PostgreSQL.
    """
    from src.load import load_silver_to_postgres

    silver_key: str = context["ti"].xcom_pull(
        task_ids="transform_silver", key="silver_key"
    )

    if not silver_key:
        raise ValueError("silver_key não encontrada no XCom.")

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://weather_user:weather_pass@postgres:5432/weather_db",
    )

    linhas = load_silver_to_postgres(db_url=db_url, silver_key=silver_key)
    logging.info(f"[load] {linhas} linha(s) inserida(s) no histórico.")


# =============================================================
# TASK 4 — Views Gold: Aplica/atualiza views analíticas
# =============================================================
def task_gold_views(**context: typing.Any) -> None:
    """
    Aplica (ou recria) todas as views analíticas de infra/postgres/gold/.
    O gold.py lê os .sql e executa CREATE OR REPLACE VIEW no PostgreSQL.
    """
    from src.gold import apply_gold_views

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://weather_user:weather_pass@postgres:5432/weather_db",
    )

    views = apply_gold_views(db_url=db_url)
    logging.info(f"[gold] {len(views)} view(s) aplicada(s): {views}")


# =============================================================
# TASK 5 — Alertas: verifica risco e dispara Telegram
# =============================================================
def task_alertas(**context: typing.Any) -> None:
    """
    Baixa o Silver mais recente do MinIO, monta o DataFrame e chama
    verificar_e_disparar_alertas(). Loga se não houver condições críticas.
    """
    import pandas as pd

    from src.alertas import verificar_e_disparar_alertas
    from src.storage import download_from_silver

    silver_key: str = context["ti"].xcom_pull(
        task_ids="transform_silver", key="silver_key"
    )

    if not silver_key:
        raise ValueError("silver_key não encontrada no XCom.")

    json_str = download_from_silver(silver_key)
    if not json_str:
        raise RuntimeError(f"Falha ao baixar Silver do MinIO: {silver_key}")

    df = pd.read_json(io.StringIO(json_str), orient="records")
    total = verificar_e_disparar_alertas(df)
    logging.info(f"[alertas] {total} alerta(s) disparado(s).")


# =============================================================
# Definição do DAG
# =============================================================
with DAG(
    dag_id="coleta_salvador",
    description="ETL de dados climáticos: OpenWeather → MinIO → PostgreSQL → Alertas",
    schedule_interval="@hourly",  # Coleta a cada hora
    start_date=datetime(2026, 5, 1),
    catchup=False,  # Não reprocessa datas passadas ao ligar
    default_args=DEFAULT_ARGS,
    tags=["weather", "etl", "salvador"],
) as dag:
    extract_bronze = PythonOperator(
        task_id="extract_bronze",
        python_callable=task_extract,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=task_transform,
    )

    load_historico = PythonOperator(
        task_id="load_historico",
        python_callable=task_load,
    )

    apply_gold = PythonOperator(
        task_id="apply_gold_views",
        python_callable=task_gold_views,
    )

    dispara_alertas = PythonOperator(
        task_id="dispara_alertas",
        python_callable=task_alertas,
    )

    # Pipeline com ramos independentes:
    # 1. Pipeline de Dados: Extrai -> Transforma -> Carrega no BD -> Atualiza Views
    # 2. Pipeline de Ação:  Extrai -> Transforma -> Dispara Alertas (nao depende do BD)
    extract_bronze >> transform_silver

    transform_silver >> load_historico >> apply_gold
    transform_silver >> dispara_alertas
