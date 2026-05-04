CREATE OR REPLACE VIEW vw_gold_estatisticas_semanais AS
SELECT
    DATE_TRUNC('week', data_hora AT TIME ZONE 'America/Bahia')   AS semana,
    cidade,
    COUNT(*)                                                        AS total_leituras,
    ROUND(AVG(temperatura_c)::numeric, 2)                           AS temp_media_c,
    MAX(temperatura_c)                                              AS temp_max_c,
    MIN(temperatura_c)                                              AS temp_min_c,
    ROUND(SUM(chuva_1h_mm)::numeric, 2)                            AS chuva_total_mm,
    ROUND(AVG(umidade_pct)::numeric, 1)                            AS umidade_media_pct,
    MAX(vento_velocidade_ms)                                       AS vento_max_ms,
    -- Mantendo sua lógica de contagem de riscos que é excelente
    COUNT(*) FILTER (WHERE nivel_risco IN ('CRÍTICO', 'ALERTA'))   AS eventos_risco_count
FROM tb_weather_history
GROUP BY DATE_TRUNC('week', data_hora AT TIME ZONE 'America/Bahia'), cidade
ORDER BY semana DESC, cidade;
