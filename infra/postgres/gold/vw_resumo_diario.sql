CREATE OR REPLACE VIEW vw_gold_resumo_diario AS
SELECT
    DATE(data_hora AT TIME ZONE 'America/Bahia')    AS data,
    cidade,
    COUNT(*)                                         AS total_leituras,
    ROUND(AVG(temperatura_c)::numeric, 2)            AS temp_media_c,
    MAX(temperatura_c)                               AS temp_max_c,
    MIN(temperatura_c)                               AS temp_min_c,
    ROUND(AVG(umidade_pct)::numeric, 1)             AS umidade_media_pct,
    ROUND(SUM(chuva_1h_mm)::numeric, 2)             AS chuva_total_mm,
    MAX(vento_velocidade_ms)                         AS vento_max_ms,
    MAX(vento_rajada_ms)                             AS rajada_max_ms
FROM tb_weather_history
GROUP BY DATE(data_hora AT TIME ZONE 'America/Bahia'), cidade
ORDER BY data DESC, cidade;
