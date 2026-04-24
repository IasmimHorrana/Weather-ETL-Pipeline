"""
test_alertas.py — Testes unitários para o módulo src/alertas.py

Biblioteca: pytest + unittest.mock (nativa do Python)

Por que Mock?
    O alertas.py tem uma dependência externa: a API do Telegram.
    Nos testes, NUNCA chamamos APIs reais porque:
      1. O teste ficaria lento (depende da internet)
      2. O teste ficaria frágil (quebraria se o Telegram caísse)
      3. Poderíamos disparar mensagens reais acidentalmente

    Com unittest.mock.patch, substituímos a função real por uma "fantoche"
    que finge ter funcionado. Assim testamos NOSSA lógica, não a do Telegram.

O que é testado:
    ✅ Situação NORMAL → nenhum alerta disparado (retorno = 0)
    ✅ Situação CRÍTICA → alerta detectado e disparado (retorno = 1)
    ✅ Situação ALERTA → alerta detectado e disparado
    ✅ DataFrame sem coluna 'nivel_risco' → retorno defensivo = 0
    ✅ DataFrame vazio → retorno = 0
    ✅ Múltiplas cidades → apenas as críticas são notificadas
    ✅ Falha no envio Telegram → pipeline não quebra (retorno = 0)
"""

import pandas as pd
import pytest
from unittest.mock import patch

# Importa as funções públicas do módulo que será testado
from src.alertas import verificar_e_disparar_alertas, _formatar_mensagem


# ─────────────────────────────────────────────
# FIXTURES: Dados reutilizáveis entre os testes
# ─────────────────────────────────────────────
# Uma "fixture" é uma função que o pytest chama automaticamente e passa
# como argumento para os testes. Evita repetição de código.

@pytest.fixture
def linha_base() -> dict:
    """Dicionário base de uma linha de dados de Salvador, situação NORMAL."""
    return {
        "cidade": "Salvador",
        "pais": "BR",
        "temperatura_c": 28.0,
        "umidade_pct": 60,
        "chuva_1h_mm": 0.5,        # Pouca chuva → NORMAL
        "vento_velocidade_ms": 5.0, # Vento fraco → NORMAL
        "nivel_risco": "NORMAL",
        "data_hora": "2026-04-24T10:00:00-03:00",
    }


@pytest.fixture
def df_normal(linha_base) -> pd.DataFrame:
    """DataFrame com situação NORMAL (não deve disparar alertas)."""
    return pd.DataFrame([linha_base])


@pytest.fixture
def df_critico() -> pd.DataFrame:
    """DataFrame com situação CRÍTICA (chuva >= 50mm → deve disparar alerta)."""
    return pd.DataFrame([{
        "cidade": "Salvador",
        "pais": "BR",
        "temperatura_c": 26.0,
        "umidade_pct": 97,
        "chuva_1h_mm": 55.0,         # Limiar CRÍTICO: >= 50mm
        "vento_velocidade_ms": 18.0,  # Limiar CRÍTICO: >= 15 m/s
        "nivel_risco": "CRÍTICO",
        "data_hora": "2026-04-24T15:00:00-03:00",
    }])


@pytest.fixture
def df_alerta() -> pd.DataFrame:
    """DataFrame com situação ALERTA (chuva >= 25mm)."""
    return pd.DataFrame([{
        "cidade": "Salvador",
        "pais": "BR",
        "temperatura_c": 27.0,
        "umidade_pct": 90,
        "chuva_1h_mm": 30.0,         # Limiar ALERTA: >= 25mm
        "vento_velocidade_ms": 10.0,
        "nivel_risco": "ALERTA",
        "data_hora": "2026-04-24T14:00:00-03:00",
    }])


# ─────────────────────────────────────────────────────────────────
# TESTES: Cada função test_ é um cenário independente
# ─────────────────────────────────────────────────────────────────

class TestVerificarEDispararAlertas:
    """Agrupa todos os testes da função principal do módulo."""

    def test_situacao_normal_nao_dispara_alerta(self, df_normal):
        """
        CENÁRIO: Clima em Salvador está tranquilo.
        ESPERADO: Função retorna 0 (nenhum alerta disparado).
        """
        resultado = verificar_e_disparar_alertas(df_normal)
        assert resultado == 0

    def test_situacao_critica_dispara_alerta(self, df_critico):
        """
        CENÁRIO: Chuva intensa e vento forte em Salvador.
        ESPERADO: Função retorna 1 (1 alerta disparado).

        O `patch` intercepta a chamada a `_enviar_telegram` ANTES de ela
        tentar se conectar ao Telegram. O MagicMock retorna True por padrão,
        simulando um envio bem-sucedido.
        """
        with patch("src.alertas._enviar_telegram", return_value=True) as mock_telegram:
            resultado = verificar_e_disparar_alertas(df_critico)

        assert resultado == 1
        # Garante que a função de envio foi de fato chamada 1 vez
        mock_telegram.assert_called_once()

    def test_situacao_alerta_dispara_notificacao(self, df_alerta):
        """
        CENÁRIO: Chuva moderada (30mm) — nível ALERTA.
        ESPERADO: Retorna 1 (ALERTA está em NIVEIS_CRITICOS).
        """
        with patch("src.alertas._enviar_telegram", return_value=True):
            resultado = verificar_e_disparar_alertas(df_alerta)

        assert resultado == 1

    def test_dataframe_sem_coluna_nivel_risco_retorna_zero(self):
        """
        CENÁRIO: O transform.py quebrou e não criou a coluna 'nivel_risco'.
        ESPERADO: Função não quebra o pipeline (retorno defensivo = 0).
        """
        df_invalido = pd.DataFrame([{"cidade": "Salvador", "chuva_1h_mm": 100.0}])
        resultado = verificar_e_disparar_alertas(df_invalido)
        assert resultado == 0

    def test_dataframe_vazio_retorna_zero(self):
        """
        CENÁRIO: API não retornou nenhum dado (DataFrame vazio).
        ESPERADO: Retorna 0 sem erros.
        """
        df_vazio = pd.DataFrame(columns=["cidade", "nivel_risco", "chuva_1h_mm"])
        resultado = verificar_e_disparar_alertas(df_vazio)
        assert resultado == 0

    def test_multiplas_cidades_so_notifica_criticas(self):
        """
        CENÁRIO: Pipeline expandido com Salvador (CRÍTICO) e Feira de Santana (NORMAL).
        ESPERADO: Apenas 1 alerta disparado (somente a cidade crítica).
        """
        df_multi = pd.DataFrame([
            {
                "cidade": "Salvador", "pais": "BR", "temperatura_c": 26.0,
                "umidade_pct": 97, "chuva_1h_mm": 55.0,
                "vento_velocidade_ms": 18.0, "nivel_risco": "CRÍTICO",
                "data_hora": "2026-04-24T15:00:00-03:00",
            },
            {
                "cidade": "Feira de Santana", "pais": "BR", "temperatura_c": 32.0,
                "umidade_pct": 45, "chuva_1h_mm": 0.0,
                "vento_velocidade_ms": 3.0, "nivel_risco": "NORMAL",
                "data_hora": "2026-04-24T15:00:00-03:00",
            },
        ])

        with patch("src.alertas._enviar_telegram", return_value=True) as mock_telegram:
            resultado = verificar_e_disparar_alertas(df_multi)

        assert resultado == 1
        assert mock_telegram.call_count == 1  # Só uma chamada ao Telegram

    def test_falha_no_telegram_nao_quebra_o_pipeline(self, df_critico):
        """
        CENÁRIO: API do Telegram está fora do ar (retorna False).
        ESPERADO: Função retorna 0 SEM lançar exceção (pipeline continua).

        Este é um teste de RESILIÊNCIA — valida que o alertas.py não
        derruba o load.py por causa de uma falha de notificação.
        """
        with patch("src.alertas._enviar_telegram", return_value=False):
            resultado = verificar_e_disparar_alertas(df_critico)

        # Alertas foram DETECTADOS (risco crítico existe), mas não DISPARADOS (telegram falhou)
        assert resultado == 0


class TestFormatarMensagem:
    """Testa a formatação do texto de alerta."""

    def test_mensagem_critica_contem_emoji_correto(self):
        """
        ESPERADO: Nível CRÍTICO usa o emoji 🚨.
        """
        row = pd.Series({
            "nivel_risco": "CRÍTICO",
            "cidade": "Salvador",
            "pais": "BR",
            "chuva_1h_mm": 55.0,
            "vento_velocidade_ms": 18.0,
            "umidade_pct": 97,
            "temperatura_c": 26.0,
            "data_hora": "2026-04-24T15:00:00-03:00",
        })
        mensagem = _formatar_mensagem(row)
        assert "🚨" in mensagem
        assert "CRÍTICO" in mensagem
        assert "Salvador" in mensagem

    def test_mensagem_alerta_contem_emoji_correto(self):
        """
        ESPERADO: Nível ALERTA usa o emoji ⚠️.
        """
        row = pd.Series({
            "nivel_risco": "ALERTA",
            "cidade": "Salvador",
            "pais": "BR",
            "chuva_1h_mm": 30.0,
            "vento_velocidade_ms": 10.0,
            "umidade_pct": 90,
            "temperatura_c": 27.0,
            "data_hora": "2026-04-24T14:00:00-03:00",
        })
        mensagem = _formatar_mensagem(row)
        assert "⚠️" in mensagem
        assert "ALERTA" in mensagem
