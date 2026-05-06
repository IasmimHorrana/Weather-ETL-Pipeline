CREATE OR REPLACE VIEW vw_gold_alertas_historico AS
SELECT
    data_hora,
    cidade,
    pais,
    nivel_risco,
    chuva_1h_mm,
    vento_velocidade_ms,
    umidade_pct,
    temperatura_c,
    condicao_clima,
    coletado_em
FROM tb_weather_history
WHERE nivel_risco IN ('CRÍTICO', 'ALERTA')
ORDER BY data_hora DESC;
