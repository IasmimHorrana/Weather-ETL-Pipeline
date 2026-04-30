import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call

from src.gold import apply_gold_views, GOLD_SQL_DIR

DB_URL = "postgresql://weather_user:weather_pass@localhost:5432/weather_db"


class TestApplyGoldViews:

    def test_aplica_todas_as_views_encontradas(self, tmp_path):
        """
        CENÁRIO: Diretório gold tem 2 arquivos .sql válidos.
        ESPERADO: Ambas as views são aplicadas e seus nomes retornados.
        """
        (tmp_path / "vw_a.sql").write_text("CREATE OR REPLACE VIEW vw_a AS SELECT 1;")
        (tmp_path / "vw_b.sql").write_text("CREATE OR REPLACE VIEW vw_b AS SELECT 2;")

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.gold.GOLD_SQL_DIR", tmp_path):
            with patch("src.gold._get_engine", return_value=mock_engine):
                resultado = apply_gold_views(db_url=DB_URL)

        assert sorted(resultado) == ["vw_a", "vw_b"]

    def test_retorna_lista_vazia_se_diretorio_sem_sql(self, tmp_path):
        """
        CENÁRIO: Diretório gold existe mas não tem arquivos .sql.
        ESPERADO: Retorna lista vazia sem lançar exceção.
        """
        with patch("src.gold.GOLD_SQL_DIR", tmp_path):
            resultado = apply_gold_views(db_url=DB_URL)

        assert resultado == []

    def test_arquivo_vazio_e_ignorado(self, tmp_path):
        """
        CENÁRIO: Um arquivo .sql está vazio (edge case de dev).
        ESPERADO: É pulado silenciosamente. A lista de retorno não inclui ele.
        """
        (tmp_path / "vw_vazia.sql").write_text("")
        (tmp_path / "vw_valida.sql").write_text("CREATE OR REPLACE VIEW vw_valida AS SELECT 1;")

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.gold.GOLD_SQL_DIR", tmp_path):
            with patch("src.gold._get_engine", return_value=mock_engine):
                resultado = apply_gold_views(db_url=DB_URL)

        assert resultado == ["vw_valida"]

    def test_falha_em_uma_view_nao_impede_as_demais(self, tmp_path):
        """
        CENÁRIO: A primeira view falha (ex: sintaxe SQL inválida).
        ESPERADO: A segunda view ainda é aplicada — degradação graceful.
        """
        (tmp_path / "vw_a.sql").write_text("SQL INVALIDO;")
        (tmp_path / "vw_b.sql").write_text("CREATE OR REPLACE VIEW vw_b AS SELECT 1;")

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        chamadas = []

        def enter_side_effect():
            chamadas.append(1)
            if len(chamadas) == 1:
                raise Exception("syntax error")
            return mock_conn

        mock_engine.begin.return_value.__enter__ = MagicMock(
            side_effect=enter_side_effect
        )

        with patch("src.gold.GOLD_SQL_DIR", tmp_path):
            with patch("src.gold._get_engine", return_value=mock_engine):
                resultado = apply_gold_views(db_url=DB_URL)

        assert resultado == ["vw_b"]

    def test_diretorio_gold_real_tem_arquivos_sql(self):
        """
        CENÁRIO: Verifica que o diretório real infra/postgres/gold/ existe
        e contém pelo menos um arquivo .sql após a implementação.
        ESPERADO: A pasta existe e tem views definidas.
        """
        assert GOLD_SQL_DIR.exists(), f"Diretório gold não encontrado: {GOLD_SQL_DIR}"
        arquivos = list(GOLD_SQL_DIR.glob("*.sql"))
        assert len(arquivos) > 0, "Nenhum arquivo .sql encontrado no diretório gold/"

    def test_views_reais_tem_create_or_replace(self):
        """
        CENÁRIO: Cada arquivo .sql no diretório gold deve conter CREATE OR REPLACE VIEW.
        ESPERADO: Garante que os arquivos são views idempotentes (re-aplicáveis).
        """
        for arquivo in GOLD_SQL_DIR.glob("*.sql"):
            conteudo = arquivo.read_text(encoding="utf-8").upper()
            assert "CREATE OR REPLACE VIEW" in conteudo, (
                f"{arquivo.name} não contém 'CREATE OR REPLACE VIEW' — "
                "views Gold devem ser idempotentes."
            )

    def test_ordem_de_aplicacao_e_alfabetica(self, tmp_path):
        """
        CENÁRIO: 3 arquivos com nomes em ordem não-alfabética.
        ESPERADO: São aplicados em ordem alfabética (comportamento determinístico).
        """
        (tmp_path / "vw_c.sql").write_text("CREATE OR REPLACE VIEW vw_c AS SELECT 3;")
        (tmp_path / "vw_a.sql").write_text("CREATE OR REPLACE VIEW vw_a AS SELECT 1;")
        (tmp_path / "vw_b.sql").write_text("CREATE OR REPLACE VIEW vw_b AS SELECT 2;")

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.gold.GOLD_SQL_DIR", tmp_path):
            with patch("src.gold._get_engine", return_value=mock_engine):
                resultado = apply_gold_views(db_url=DB_URL)

        assert resultado == ["vw_a", "vw_b", "vw_c"]
