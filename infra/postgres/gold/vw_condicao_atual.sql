-- vw_gold_condicao_atual
-- Retorna APENAS a leitura mais recente de cada cidade.
-- Uso no Metabase: cards de "Condição Atual" no topo do dashboard —
-- um card por cidade mostrando temperatura, risco e descrição do clima agora.

CREATE OR REPLACE VIEW vw_gold_condicao_atual AS
SELECT DISTINCT ON (cidade)
    cidade,
    pais,
    data_hora,
    temperatura_c,
    sensacao_termica_c,
    umidade_pct,
    chuva_1h_mm,
    vento_velocidade_ms,
    vento_rajada_ms,
    nuvens_pct,
    visibilidade_m,
    condicao_clima,
    descricao_clima,
    nivel_risco,
    coletado_em
FROM tb_weather_history
ORDER BY cidade, data_hora DESC;
