import json
from unittest.mock import MagicMock, patch

import pytest

from src.load import COLUNAS_EXCLUIR, load_silver_to_postgres


@pytest.fixture
def silver_json() -> str:
    """JSON Silver mínimo válido, no formato que o MinIO retornaria."""
    dados = [
        {
            "cidade": "Salvador",
            "pais": "BR",
            "latitude": -12.97,
            "longitude": -38.50,
            "temperatura_c": 28.5,
            "sensacao_termica_c": 30.1,
            "temp_min_c": 25.0,
            "temp_max_c": 31.0,
            "pressao_hpa": 1012,
            "umidade_pct": 82,
            "visibilidade_m": 6000,
            "vento_velocidade_ms": 7.2,
            "vento_direcao_grau": 160,
            "vento_rajada_ms": 9.1,
            "chuva_1h_mm": 0.5,
            "nuvens_pct": 57,
            "condicao_clima": "Rain",
            "descricao_clima": "chuva leve",
            "nivel_risco": "NORMAL",
            # Colunas que o banco gerencia (devem ser removidas antes do insert)
            "id": 999,
            "coletado_em": "2026-04-30T10:00:00",
            "timezone": -10800,
        }
    ]
    return json.dumps(dados)


@pytest.fixture
def mock_engine():
    """
    Engine SQLAlchemy completamente mockado.
    Suporta `with engine.begin() as conn` (context manager).
    """
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


DB_URL = "postgresql://user:pass@localhost:5432/weather_db"


# ─────────────────────────────────────────────
# TESTES
# ─────────────────────────────────────────────


class TestLoadSilverToPostgres:
    def test_sucesso_via_minio_retorna_qtd_linhas(self, silver_json, mock_engine):
        """
        CENÁRIO: silver_key fornecida, MinIO e banco disponíveis.
        ESPERADO: Retorna 1 (1 linha inserida).
        """
        engine, conn = mock_engine
        conn.execute.return_value.rowcount = 1

        mock_stmt = MagicMock()
        mock_stmt.values.return_value.on_conflict_do_nothing.return_value = mock_stmt
        mock_insert = MagicMock(return_value=mock_stmt)
        mock_meta = MagicMock()

        with patch("src.load.download_from_silver", return_value=silver_json):
            with patch("src.load._get_engine", return_value=engine):
                with patch("src.load.MetaData", return_value=mock_meta):
                    with patch("src.load.Table", return_value=MagicMock()):
                        with patch("src.load.insert", mock_insert):
                            resultado = load_silver_to_postgres(
                                db_url=DB_URL,
                                silver_key="weather_silver/2026-04-30/10-00-00_salvador.json",
                            )

        assert resultado == 1

    def test_sucesso_via_arquivo_local_retorna_qtd_linhas(
        self, silver_json, tmp_path, mock_engine
    ):
        """
        CENÁRIO: input_path fornecido (modo dev), sem MinIO.
        ESPERADO: Retorna 1 (1 linha inserida do arquivo local).
        """
        engine, conn = mock_engine
        conn.execute.return_value.rowcount = 1
        arquivo = tmp_path / "weather_silver.json"
        arquivo.write_text(silver_json, encoding="utf-8")

        mock_stmt = MagicMock()
        mock_stmt.values.return_value.on_conflict_do_nothing.return_value = mock_stmt
        mock_insert = MagicMock(return_value=mock_stmt)

        with patch("src.load._get_engine", return_value=engine):
            with patch("src.load.MetaData", return_value=MagicMock()):
                with patch("src.load.Table", return_value=MagicMock()):
                    with patch("src.load.insert", mock_insert):
                        resultado = load_silver_to_postgres(
                            db_url=DB_URL, input_path=arquivo
                        )

        assert resultado == 1

    def test_sem_silver_key_e_sem_path_lanca_value_error(self):
        """
        CENÁRIO: Nenhuma fonte fornecida.
        ESPERADO: ValueError com mensagem clara.
        """
        with pytest.raises(ValueError, match="Forneça silver_key"):
            load_silver_to_postgres(db_url=DB_URL)

    def test_minio_falha_retorna_zero(self):
        """
        CENÁRIO: download_from_silver retorna '' (MinIO indisponível).
        ESPERADO: Retorna 0 sem quebrar.
        """
        with patch("src.load.download_from_silver", return_value=""):
            resultado = load_silver_to_postgres(
                db_url=DB_URL,
                silver_key="chave_invalida.json",
            )
        assert resultado == 0

    def test_arquivo_local_inexistente_retorna_zero(self, tmp_path):
        """
        CENÁRIO: input_path aponta para arquivo que não existe.
        ESPERADO: Retorna 0 sem lançar exceção não tratada.
        """
        caminho_falso = tmp_path / "nao_existe.json"
        resultado = load_silver_to_postgres(db_url=DB_URL, input_path=caminho_falso)
        assert resultado == 0

    def test_colunas_gerenciadas_pelo_banco_sao_removidas(
        self, silver_json, mock_engine
    ):
        """
        CENÁRIO: JSON Silver contém id, coletado_em, timezone.
        ESPERADO: O dict enviado ao insert não contém essas colunas.
        """
        engine, conn = mock_engine
        conn.execute.return_value.rowcount = 1
        registros_recebidos = {}

        mock_stmt_final = MagicMock()
        mock_stmt_final.on_conflict_do_nothing.return_value = mock_stmt_final

        mock_stmt_base = MagicMock()
        mock_stmt_base.values.side_effect = lambda registros: (
            registros_recebidos.update({"dados": registros}) or mock_stmt_final
        )

        mock_insert = MagicMock(return_value=mock_stmt_base)

        with patch("src.load.download_from_silver", return_value=silver_json):
            with patch("src.load._get_engine", return_value=engine):
                with patch("src.load.MetaData", return_value=MagicMock()):
                    with patch("src.load.Table", return_value=MagicMock()):
                        with patch("src.load.insert", mock_insert):
                            load_silver_to_postgres(
                                db_url=DB_URL,
                                silver_key="chave.json",
                            )

        dados = registros_recebidos.get("dados")
        assert dados is not None
        for col in COLUNAS_EXCLUIR:
            for row in dados:
                assert col not in row, f"Coluna '{col}' não deveria estar no insert"

    def test_erro_no_banco_retorna_zero(self, silver_json):
        """
        CENÁRIO: Falha transacional no PostgreSQL (ex: tabela não existe).
        ESPERADO: Retorna 0 sem propagar a exceção.
        """
        engine = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(
            side_effect=Exception("relation does not exist")
        )
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.load.download_from_silver", return_value=silver_json):
            with patch("src.load._get_engine", return_value=engine):
                resultado = load_silver_to_postgres(
                    db_url=DB_URL,
                    silver_key="chave.json",
                )

        assert resultado == 0
