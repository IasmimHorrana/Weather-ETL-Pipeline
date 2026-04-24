"""
alertas.py — Módulo de Notificação.

Responsabilidade única: receber um DataFrame já processado (Camada Silver)
e disparar notificações externas quando as condições climáticas atingem
limiares de risco definidos pela regra de negócio do transform.py.

Decisão de Arquitetura:
    Separar a lógica de ALERTA da lógica de TRANSFORMAÇÃO (transform.py) é
    fundamental para escalabilidade. Depois, se quisermos adicionar um novo
    canal, mexemos APENAS neste arquivo, sem tocar na regra de negócio central.
"""

import logging
import os
import requests
import pandas as pd

# Lê o token e o chat_id do bot do Telegram de variáveis de ambiente.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Níveis que disparam uma notificação ativa.
NIVEIS_CRITICOS = {"CRÍTICO", "ALERTA"}


def _formatar_mensagem(row: pd.Series) -> str:
    """
    Formata uma linha do DataFrame em uma mensagem legível para humanos.

    Função privada (prefixo '_') pois é um detalhe de implementação interno.
    Retorna uma string pronta para ser enviada via qualquer canal.
    """
    emoji = "🚨" if row["nivel_risco"] == "CRÍTICO" else "⚠️"

    return (
        f"{emoji} *ALERTA METEOROLÓGICO — {row['nivel_risco']}*\n\n"
        f"📍 Cidade: {row['cidade']} ({row.get('pais', 'BR')})\n"
        f"🌧️ Chuva (1h): {row['chuva_1h_mm']} mm\n"
        f"💨 Vento: {row['vento_velocidade_ms']} m/s\n"
        f"💧 Umidade: {row['umidade_pct']}%\n"
        f"🌡️ Temperatura: {row['temperatura_c']}°C\n"
        f"🕐 Data/Hora: {row['data_hora']}"
    )


def _enviar_telegram(mensagem: str) -> bool:
    """
    Faz um POST na API do Telegram para entregar a mensagem ao chat configurado.

    Retorna True em caso de sucesso, False em caso de falha (sem quebrar o pipeline).
    A ideia de retornar bool (ao invés de raise Exception) é proposital:
    um erro de notificação NÃO deve interromper o salvamento dos dados no banco.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning(
            "Credenciais do Telegram não configuradas. "
            "Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID no ambiente."
        )
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensagem,
        "parse_mode": "Markdown",  # Permite negrito/itálico na mensagem
    }

    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()  # Levanta exceção para status 4xx/5xx
        logging.info("Mensagem de alerta enviada com sucesso via Telegram.")
        return True
    except requests.exceptions.RequestException as e:
        # Loga o erro mas não propaga — o pipeline deve continuar mesmo assim.
        logging.error(f"Falha ao enviar alerta via Telegram: {e}")
        return False


def verificar_e_disparar_alertas(df: pd.DataFrame) -> int:
    """
    Função principal do módulo. Recebe o DataFrame da Camada Silver,
    filtra as linhas com risco elevado e dispara as notificações.

    Retorna o número de alertas disparados.

    Para adicionar um novo canal de notificação, basta chamar a
    nova função de envio aqui, sem alterar a lógica de filtro.
    """
    logging.info("Iniciando verificação de alertas climáticos.")

    # Verifica se a coluna existe antes de filtrar.
    if "nivel_risco" not in df.columns:
        logging.error("Coluna 'nivel_risco' não encontrada no DataFrame. O transform.py rodou?")
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
            f"RISCO {row['nivel_risco']} detectado em {row['cidade']}! "
            f"Chuva: {row['chuva_1h_mm']}mm | Vento: {row['vento_velocidade_ms']}m/s"
        )

        mensagem = _formatar_mensagem(row)

        # Canal Telegram
        if _enviar_telegram(mensagem):
            alertas_disparados += 1

    logging.info(f"Verificação concluída. Total de alertas disparados: {alertas_disparados}")
    return alertas_disparados


# --- Teste Manual ---
if __name__ == "__main__":
    # Simula um DataFrame com uma situação CRÍTICA para validar o módulo
    dados_simulados = pd.DataFrame([{
        "cidade": "Salvador",
        "pais": "BR",
        "temperatura_c": 28.0,
        "umidade_pct": 95,
        "chuva_1h_mm": 55.0,      # Acima de 50mm → CRÍTICO
        "vento_velocidade_ms": 18.0,  # Acima de 15m/s → CRÍTICO
        "nivel_risco": "CRÍTICO",
        "data_hora": "2026-04-23T21:00:00-03:00",
    }])

    total = verificar_e_disparar_alertas(dados_simulados)
    print(f"\nTotal de alertas processados: {total}")
