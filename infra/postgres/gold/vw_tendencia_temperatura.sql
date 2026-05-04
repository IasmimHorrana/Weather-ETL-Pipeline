CREATE OR REPLACE VIEW vw_gold_tendencia_temperatura AS
SELECT
    data_hora,
    cidade,
    temperatura_c,
    sensacao_termica_c,
    temp_min_c,
    temp_max_c,
    chuva_1h_mm,
    -- Média móvel das últimas 3 leituras (suaviza oscilações da API)
    ROUND(
        AVG(temperatura_c) OVER (
            PARTITION BY cidade
            ORDER BY data_hora
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )::numeric, 2
    ) AS temp_media_movel_3h,
    nivel_risco
FROM tb_weather_history
ORDER BY cidade, data_hora DESC;
