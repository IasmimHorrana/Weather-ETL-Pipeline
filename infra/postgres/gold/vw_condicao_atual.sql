CREATE OR REPLACE VIEW vw_gold_condicao_atual AS
SELECT DISTINCT ON (cidade)
    cidade,
    pais,
    data_hora,
    temperatura_c,
    umidade_pct,
    chuva_1h_mm,
    vento_velocidade_ms,
    condicao_clima,
    nivel_risco,
    coletado_em
FROM tb_weather_history
ORDER BY cidade, data_hora DESC;
