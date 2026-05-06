-- Script de Inicialização do Banco de Dados

-- Este arquivo é executado AUTOMATICAMENTE pelo contêiner do
-- PostgreSQL na PRIMEIRA vez que ele sobe (quando o volume
-- ainda está vazio)

-- Conecta ao banco 'weather_db' para criar os objetos dentro dele.
-- (Este banco já é criado automaticamente pelo Docker Compose via variável POSTGRES_DB)
\c weather_db;

-- Garante que esta sessão interpreta strings com acentos corretamente,
-- independente do locale do sistema operacional do host.
SET CLIENT_ENCODING TO 'UTF8';

CREATE TABLE IF NOT EXISTS tb_weather_history (

    -- CHAVE PRIMÁRIA: gerada automaticamente pelo banco.
    -- SERIAL = auto-incremento (1, 2, 3...). Cada linha tem um ID único.
    id               SERIAL PRIMARY KEY,

    -- IDENTIFICAÇÃO GEOGRÁFICA
    cidade           VARCHAR(100)    NOT NULL,   -- "Salvador"
    pais             VARCHAR(5),                  -- "BR"
    latitude         NUMERIC(9, 6),               -- -12.9711 (6 casas para precisão GPS)
    longitude        NUMERIC(9, 6),               -- -38.5108

    -- TEMPORAL: quando a leitura foi feita pela API
    -- TIMESTAMPTZ = "Timestamp with Time Zone" — preserva o fuso America/Bahia
    data_hora        TIMESTAMPTZ     NOT NULL,
    nascer_sol       TIMESTAMPTZ,
    por_sol          TIMESTAMPTZ,

    -- TEMPERATURA (graus Celsius)
    temperatura_c    NUMERIC(5, 2),               -- 26.48°C
    sensacao_termica_c NUMERIC(5, 2),             -- sensação térmica
    temp_min_c       NUMERIC(5, 2),
    temp_max_c       NUMERIC(5, 2),

    -- CONDIÇÕES ATMOSFÉRICAS
    pressao_hpa      INTEGER,                      -- pressão em hPa (ex: 1012)
    umidade_pct      INTEGER,                      -- umidade em % (0 a 100)
    visibilidade_m   INTEGER,                      -- visibilidade em metros

    -- VENTO
    vento_velocidade_ms  NUMERIC(6, 2),            -- m/s
    vento_direcao_grau   INTEGER,                  -- graus (0 a 360)
    vento_rajada_ms      NUMERIC(6, 2),

    -- PRECIPITAÇÃO E COBERTURA
    chuva_1h_mm      NUMERIC(7, 2)   DEFAULT 0.0, -- mm de chuva na última hora
    nuvens_pct       INTEGER,                      -- % de cobertura de nuvens

    -- CONDIÇÃO TEXTUAL (da API)
    condicao_clima   VARCHAR(50),                  -- "Rain", "Clear", "Clouds"
    descricao_clima  VARCHAR(100),                 -- "chuva leve", "céu limpo"

    -- REGRA DE NEGÓCIO (calculada pelo transform.py)
    -- O CHECK garante que o banco rejeite qualquer valor fora do contrato.
    -- Os valores com acento (ATENÇÃO, CRÍTICO) são preservados pois o banco
    -- é criado com ENCODING UTF8 pelo Docker Compose (POSTGRES_INITDB_ARGS).
    nivel_risco      VARCHAR(10)
                     CHECK (nivel_risco IN ('NORMAL', 'ATENÇÃO', 'ALERTA', 'CRÍTICO')),

    -- AUDITORIA: quando ESTE REGISTRO foi inserido no banco
    -- DEFAULT NOW() = o banco preenche automaticamente com a hora atual da inserção
    -- Permite rastrear atrasos entre coleta e carga — útil para debugar o Airflow
    coletado_em      TIMESTAMPTZ     DEFAULT NOW(),

    -- IDEMPOTÊNCIA: impede linhas duplicadas em caso de reprocessamento.
    -- Se o pipeline rodar duas vezes para o mesmo snapshot (mesma cidade + mesmo
    -- instante da API), o banco rejeita a segunda inserção silenciosamente.
    -- O load.py usa INSERT ... ON CONFLICT DO NOTHING para tratar isso.
    CONSTRAINT uq_weather_cidade_datahora UNIQUE (cidade, data_hora)

);

-- ÍNDICES: Aceleram as consultas mais frequentes do Metabase

-- Índice composto para séries temporais por cidade
CREATE INDEX IF NOT EXISTS idx_weather_cidade_data
    ON tb_weather_history (cidade, data_hora DESC);

-- Índice isolado para filtros só por data 
CREATE INDEX IF NOT EXISTS idx_weather_data_hora
    ON tb_weather_history (data_hora DESC);

-- Índice para dashboards de monitoramento de risco 
CREATE INDEX IF NOT EXISTS idx_weather_nivel_risco
    ON tb_weather_history (nivel_risco);
