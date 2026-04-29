"""Dispara notificações (Telegram) quando o nível de risco climático é ALERTA ou CRÍTICO."""

import functools
import logging
import os

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

# Lê o token e o chat_id do bot do Telegram de variáveis de ambiente.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Níveis que disparam uma notificação ativa.
NIVEIS_CRITICOS = {"CRÍTICO", "ALERTA"}


def _formatar_mensagem(row: pd.Series) -> str:
    """Formata uma linha do DataFrame em mensagem Markdown para notificação."""
    emoji = "🚨" if row.get("nivel_risco") == "CRÍTICO" else "⚠️"

    return (
        f"{emoji} *ALERTA METEOROLÓGICO — {row.get('nivel_risco', 'DESCONHECIDO')}*\n\n"
        f"📍 Cidade: {row.get('cidade', 'Desconhecida')} ({row.get('pais', 'BR')})\n"
        f"🌧️ Chuva (1h): {row.get('chuva_1h_mm', 0.0)} mm\n"
        f"💨 Vento: {row.get('vento_velocidade_ms', 0.0)} m/s\n"
        f"💧 Umidade: {row.get('umidade_pct', 0)}%\n"
        f"🌡️ Temperatura: {row.get('temperatura_c', 0.0)}°C\n"
        f"🕐 Data/Hora: {row.get('data_hora', 'N/A')}"
    )


@functools.lru_cache(maxsize=1)
def _validar_credenciais() -> tuple[str, str]:
    """Valida as credenciais do Telegram. Levanta ValueError se ausentes."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning(
            "Credenciais do Telegram não configuradas. "
            "Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID no ambiente."
        )
        raise ValueError("Credenciais ausentes")
    return TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def _fazer_requisicao_telegram(url: str, payload: dict) -> None:
    """Faz requisição ao Telegram com 3 tentativas e intervalo de 2s."""
    response = requests.post(url, data=payload, timeout=10)
    response.raise_for_status()


def _enviar_telegram(mensagem: str) -> bool:
    """Envia mensagem via Telegram. Retorna True em sucesso, False em falha (sem propagar)."""
    try:
        token, chat_id = _validar_credenciais()
    except ValueError:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensagem,
        "parse_mode": "Markdown",  # Permite negrito/itálico na mensagem
    }

    try:
        _fazer_requisicao_telegram(url, payload)
        logging.info("Mensagem de alerta enviada com sucesso via Telegram.")
        return True
    except Exception as e:
        # Loga o erro mas não propaga — o pipeline deve continuar mesmo assim.
        logging.error(f"Falha ao enviar alerta via Telegram (após tentativas): {e}")
        return False


def verificar_e_disparar_alertas(df: pd.DataFrame) -> int:
    """
    Filtra linhas com risco elevado e dispara notificações.

    Retorna o total de alertas disparados.
    """
    logging.info("Iniciando verificação de alertas climáticos.")

    # Verifica se a coluna existe antes de filtrar.
    if "nivel_risco" not in df.columns:
        logging.error(
            "Coluna 'nivel_risco' não encontrada no DataFrame. O transform.py rodou?"
        )
        return 0

    # Filtra apenas as linhas que requerem ação imediata
    df_alertas = df[df["nivel_risco"].isin(NIVEIS_CRITICOS)]

    if df_alertas.empty:
        logging.info("Nenhuma condição crítica detectada. Situação: NORMAL ✅")
        return 0

    alertas_disparados = 0

    # Itera apenas sobre as linhas críticas
    for _, row in df_alertas.iterrows():
        logging.warning(
            f"RISCO {row.get('nivel_risco', 'DESCONHECIDO')} detectado em {row.get('cidade', 'Desconhecida')}! "
            f"Chuva: {row.get('chuva_1h_mm', 0.0)}mm | Vento: {row.get('vento_velocidade_ms', 0.0)}m/s"
        )

        mensagem = _formatar_mensagem(row)

        # Canal Telegram
        if _enviar_telegram(mensagem):
            alertas_disparados += 1

    logging.info(
        f"Verificação concluída. Total de alertas disparados: {alertas_disparados}"
    )
    return alertas_disparados


# --- Teste Manual ---
if __name__ == "__main__":
    # Simula um DataFrame com uma situação CRÍTICA para validar o módulo
    dados_simulados = pd.DataFrame(
        [
            {
                "cidade": "Salvador",
                "pais": "BR",
                "temperatura_c": 28.0,
                "umidade_pct": 95,
                "chuva_1h_mm": 55.0,  # Acima de 50mm → CRÍTICO
                "vento_velocidade_ms": 18.0,  # Acima de 15m/s → CRÍTICO
                "nivel_risco": "CRÍTICO",
                "data_hora": "2026-04-23T21:00:00-03:00",
            }
        ]
    )

    total = verificar_e_disparar_alertas(dados_simulados)
    print(f"\nTotal de alertas processados: {total}")
