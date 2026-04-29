"""
load.py — Camada de Carga (Load) do Pipeline ETL.

Responsabilidade única: ler o JSON da Camada Silver e inserir
os dados no PostgreSQL usando a estratégia APPEND (histórico).

Decisões de Arquitetura:
    - APPEND (if_exists='append'): cada execução adiciona novas linhas,
      preservando o histórico completo para análise temporal no Metabase.
    - Variável de ambiente DATABASE_URL: permite que o mesmo código funcione
      tanto localmente (localhost) quanto dentro do Docker (nome do serviço).
    - dtype explícito: garante que o Pandas não infira tipos errados ao
      escrever no Postgres (ex: um inteiro sendo criado como BIGINT).
"""

import functools
import io
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Numeric, String

from sqlalchemy.types import Integer, Numeric, String

from src.storage import download_from_silver

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

@functools.lru_cache(maxsize=1)
def _get_engine(db_url: str):
    """Cria e cacheia o engine SQLAlchemy. Singleton por URL."""
    return create_engine(db_url)

# =============================================================
# MAPEAMENTO DE TIPOS: Pandas → PostgreSQL
# =============================================================
# Sem esse mapa, o to_sql() infere os tipos e pode criar colunas
# com tipos errados. Ex: VARCHAR sem limite de tamanho → TEXT.
# Aqui forçamos os tipos exatos que declaramos no init.sql.
DTYPE_MAP = {
    "cidade": String(100),
    "pais": String(5),
    "latitude": Numeric(9, 6),
    "longitude": Numeric(9, 6),
    "temperatura_c": Numeric(5, 2),
    "sensacao_termica_c": Numeric(5, 2),
    "temp_min_c": Numeric(5, 2),
    "temp_max_c": Numeric(5, 2),
    "pressao_hpa": Integer(),
    "umidade_pct": Integer(),
    "visibilidade_m": Integer(),
    "vento_velocidade_ms": Numeric(6, 2),
    "vento_direcao_grau": Integer(),
    "vento_rajada_ms": Numeric(6, 2),
    "chuva_1h_mm": Numeric(7, 2),
    "nuvens_pct": Integer(),
    "condicao_clima": String(50),
    "descricao_clima": String(100),
    "nivel_risco": String(10),
}

# Colunas que o init.sql gerencia automaticamente (não enviamos pelo Pandas)
# - 'id' → SERIAL PRIMARY KEY (auto-incremento)
# - 'coletado_em' → DEFAULT NOW() (preenchido pelo banco)
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
        raise ValueError("Forneça silver_key (produção) ou input_path (desenvolvimento).")

    logging.info(f"Silver carregado: {len(df)} linha(s), {len(df.columns)} colunas.")

    # 2. Remove colunas gerenciadas pelo banco (id, coletado_em, timezone)
    colunas_para_remover = [c for c in COLUNAS_EXCLUIR if c in df.columns]
    if colunas_para_remover:
        df = df.drop(columns=colunas_para_remover)
        logging.info(f"Colunas excluídas antes da carga: {colunas_para_remover}")

    # 3. Filtra o dtype_map para conter apenas colunas que existem no df
    dtype_para_uso = {col: tipo for col, tipo in DTYPE_MAP.items() if col in df.columns}

    # 4. Obtenção do engine e teste de conexão
    logging.info("Estabelecendo conexão com o PostgreSQL...")
    try:
        engine = _get_engine(db_url)
    except Exception as e:
        logging.error(f"Não foi possível inicializar engine do banco: {e}")
        return 0

    # 5. Inserção atômica no banco
    try:
        with engine.begin() as conn:
            df.to_sql(
                name="tb_weather_history",
                con=conn,
                if_exists="append",
                index=False,
                dtype=dtype_para_uso,
            )
        logging.info(
            f"[✔] Carga atômica finalizada! {len(df)} linha(s) adicionada(s) ao histórico."
        )
        return len(df)

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
        print(f"\n[Testando integração] Iniciando Carga a partir do MinIO: {silver_key_recente}")
        linhas = load_silver_to_postgres(db_url=POSTGRES_URL, silver_key=silver_key_recente)
    else:
        print("\n[Modo Local] Nenhum arquivo Silver no MinIO. Rodando com fallback local.")
        arquivo_silver = Path(__file__).parent.parent / "data" / "weather_silver.json"
        linhas = load_silver_to_postgres(db_url=POSTGRES_URL, input_path=arquivo_silver)

    print(f"\nTotal de linhas carregadas: {linhas}")
