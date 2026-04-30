-- vw_gold_estatisticas_mensais
-- Agrega métricas por mês e cidade para análise de tendências de longo prazo.
-- Uso no Metabase: gráfico de barras agrupadas por mês — comparativo mensal
-- de chuva, temperatura e número de eventos críticos.

CREATE OR REPLACE VIEW vw_gold_estatisticas_mensais AS
SELECT
    DATE_TRUNC('month', data_hora AT TIME ZONE 'America/Bahia')   AS mes,
    cidade,
    COUNT(*)                                                        AS total_leituras,
    ROUND(AVG(temperatura_c)::numeric, 2)                          AS temp_media_c,
    MAX(temperatura_c)                                             AS temp_max_c,
    MIN(temperatura_c)                                             AS temp_min_c,
    ROUND(SUM(chuva_1h_mm)::numeric, 2)                           AS chuva_total_mm,
    ROUND(AVG(umidade_pct)::numeric, 1)                           AS umidade_media_pct,
    MAX(vento_velocidade_ms)                                       AS vento_max_ms,
    -- Conta quantas leituras foram CRÍTICO ou ALERTA no mês
    COUNT(*) FILTER (WHERE nivel_risco IN ('CRÍTICO', 'ALERTA'))   AS eventos_risco_count
FROM tb_weather_history
GROUP BY DATE_TRUNC('month', data_hora AT TIME ZONE 'America/Bahia'), cidade
ORDER BY mes DESC, cidade;
