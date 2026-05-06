CREATE OR REPLACE VIEW vw_gold_tendencia_temperatura AS
SELECT
    data_hora,
    cidade,
    temperatura_c,
    chuva_1h_mm,
    -- Média móvel das últimas 3 leituras (suaviza oscilações da API)
    ROUND(
        AVG(temperatura_c) OVER (
            PARTITION BY cidade
            ORDER BY data_hora
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 2
    ) AS temp_media_movel_3h,
    -- Substitui a leitura falha da API pelas extremas REAIS calculadas durante o dia
    MIN(temperatura_c) OVER (
        PARTITION BY cidade, DATE(data_hora AT TIME ZONE 'America/Bahia')
    ) AS temp_min_dia,
    MAX(temperatura_c) OVER (
        PARTITION BY cidade, DATE(data_hora AT TIME ZONE 'America/Bahia')
    ) AS temp_max_dia,
    nivel_risco
FROM tb_weather_history
ORDER BY cidade, data_hora DESC;
