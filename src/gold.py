import functools
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Diretório onde ficam os arquivos .sql das views Gold
GOLD_SQL_DIR = Path(__file__).parent.parent / "infra" / "postgres" / "gold"


@functools.lru_cache(maxsize=1)
def _get_engine(db_url: str):
    """Cria e cacheia o engine SQLAlchemy. Singleton por URL."""
    return create_engine(db_url)


def apply_gold_views(db_url: str) -> list[str]:
    """
    Lê todos os arquivos .sql de infra/postgres/gold/ e os aplica no banco.

    Cada arquivo deve conter um único CREATE OR REPLACE VIEW.
    A ordem de aplicação é alfabética pelo nome do arquivo.

    Retorna a lista de views aplicadas com sucesso.
    """
    arquivos_sql = sorted(GOLD_SQL_DIR.glob("*.sql"))

    if not arquivos_sql:
        logging.warning(f"Nenhum arquivo .sql encontrado em: {GOLD_SQL_DIR}")
        return []

    logging.info(f"[Gold] {len(arquivos_sql)} view(s) encontrada(s) para aplicar.")

    engine = _get_engine(db_url)
    views_aplicadas: list[str] = []

    for arquivo in arquivos_sql:
        sql = arquivo.read_text(encoding="utf-8").strip()
        if not sql:
            logging.warning(f"[Gold] Arquivo vazio ignorado: {arquivo.name}")
            continue

        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            logging.info(f"[✔] View aplicada: {arquivo.stem}")
            views_aplicadas.append(arquivo.stem)
        except Exception as e:
            logging.error(f"[X] Falha ao aplicar {arquivo.name}: {e}")

    logging.info(
        f"[Gold] Concluído: {len(views_aplicadas)}/{len(arquivos_sql)} view(s) aplicada(s)."
    )
    return views_aplicadas


# --- ÁREA DE EXECUÇÃO ---
if __name__ == "__main__":
    POSTGRES_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://weather_user:weather_pass@localhost:5432/weather_db",
    )

    print("\n[Gold] Aplicando views da camada Gold no PostgreSQL...\n")
    views = apply_gold_views(db_url=POSTGRES_URL)

    if views:
        print(f"\n[✔] {len(views)} view(s) criada(s)/atualizada(s):")
        for v in views:
            print(f"    • {v}")
    else:
        print("\n[!] Nenhuma view foi aplicada. Verifique os logs acima.")
