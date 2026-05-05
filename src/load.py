import functools
import io
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from src.storage import download_from_silver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


@functools.lru_cache(maxsize=1)
def _get_engine(db_url: str) -> Engine:
    """Cria e cacheia o engine SQLAlchemy. Singleton por URL."""
    return create_engine(db_url)


# Colunas que o init.sql gerencia automaticamente
COLUNAS_EXCLUIR = ["id", "coletado_em", "timezone"]


def load_silver_to_postgres(
    db_url: str,
    silver_key: str | None = None,
    input_path: str | Path | None = None,
) -> int:
    """
    Lê o JSON da camada Silver e insere no PostgreSQL usando atomicidade.

    Retorna o número de linhas carregadas.
    """
    logging.info("Iniciando a carga de dados.")

    # 1. Leitura do Silver JSON
    if silver_key:
        logging.info(f"Modo produção: lendo Silver do MinIO ({silver_key})")
        json_str = download_from_silver(silver_key)
        if not json_str:
            logging.error(f"Falha ao baixar {silver_key} do MinIO.")
            return 0
        df = pd.read_json(io.StringIO(json_str), orient="records")
    elif input_path:
        path = Path(input_path)
        if not path.exists():
            logging.error("Arquivo Silver local não encontrado.")
            return 0
        df = pd.read_json(path, orient="records")
    else:
        raise ValueError(
            "Forneça silver_key (produção) ou input_path (desenvolvimento)."
        )

    logging.info(f"Silver carregado: {len(df)} linha(s), {len(df.columns)} colunas.")

    # 2. Remove colunas gerenciadas pelo banco (id, coletado_em, timezone)
    colunas_para_remover = [c for c in COLUNAS_EXCLUIR if c in df.columns]
    if colunas_para_remover:
        df = df.drop(columns=colunas_para_remover)
        logging.info(f"Colunas excluídas antes da carga: {colunas_para_remover}")

    # 3. Inserção idêmpotente: ON CONFLICT DO NOTHING

    try:
        engine = _get_engine(db_url)
        meta = MetaData()
        meta.reflect(bind=engine, only=["tb_weather_history"])
        tabela = Table("tb_weather_history", meta)

        registros = df.to_dict(orient="records")
        stmt = (
            insert(tabela)
            .values(registros)
            .on_conflict_do_nothing(index_elements=["cidade", "data_hora"])
        )

        with engine.begin() as conn:
            resultado = conn.execute(stmt)

        inseridas = int(resultado.rowcount)
        ignoradas = len(df) - inseridas

        if ignoradas:
            logging.info(
                f"[~] {ignoradas} linha(s) ignorada(s) por já existirem no banco."
            )
        logging.info(
            f"[✔] Carga finalizada! {inseridas} linha(s) adicionada(s) ao histórico."
        )
        return inseridas

    except Exception as e:
        logging.error(f"[X] Falha transacional ao carregar no banco de dados: {e}")
        return 0


# --- ÁREA DE EXECUÇÃO ---
if __name__ == "__main__":
    from src.storage import list_silver_files

    # DATABASE_URL é lida do ambiente.
    POSTGRES_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://weather_user:weather_pass@localhost:5432/weather_db",
    )

    # Tenta usar o arquivo mais recente no MinIO (esteira completa)
    arquivos_silver = list_silver_files()
    silver_key_recente = arquivos_silver[-1] if arquivos_silver else None

    if silver_key_recente:
        print(
            f"\n[Testando integração] Iniciando Carga a partir do MinIO: {silver_key_recente}"
        )
        linhas = load_silver_to_postgres(
            db_url=POSTGRES_URL, silver_key=silver_key_recente
        )
    else:
        print(
            "\n[Modo Local] Nenhum arquivo Silver no MinIO. Rodando com fallback local."
        )
        arquivo_silver = Path(__file__).parent.parent / "data" / "weather_silver.json"
        linhas = load_silver_to_postgres(db_url=POSTGRES_URL, input_path=arquivo_silver)

    print(f"\nTotal de linhas carregadas: {linhas}")
