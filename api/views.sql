-- Criar uma view materializada para armazenar os indicadores financeiros
CREATE MATERIALIZED VIEW IF NOT EXISTS Indicadores AS
-- Agrupar os dados por id_empresa, data_inicio e data_fim
SELECT
    e.id_empresa,
    e.nome_empresa,
    r.data_inicio,
    r.data_fim,
    -- Calcular o lucro liquido que é igual
    -- onde o tipo de relatorio é "Demonstração de Resultado" e a descricao do dado é 'Atribuído a Sócios da Empresa Controladora'
    SUM(CASE WHEN r.tipo_relatorio = 'Demonstração de Resultado' AND dr.descricao_dado = 'Atribuído a Sócios da Empresa Controladora' THEN dr.valor_dado ELSE 0 END) AS lucro_liquido,
FROM
    relatorio r JOIN dados_relatorio dr ON r.id_relatorio = dr.id_relatorio JOIN empresa e ON r.id_empresa = e.id_empresa
