CREATE INDEX idx_cnpj_base_empresas ON empresas (cnpj_base);
CREATE INDEX idx_cnpj_base_estabelecimentos ON estabelecimentos (cnpj_base);
CREATE INDEX idx_cnpj_base_simples ON simples (cnpj_base);
CREATE INDEX idx_cnpj_base_esta ON estabelecimentos (cnpj_base);
CREATE INDEX idx_cnpj_base_socios ON socios(cnpj_base);
CREATE INDEX idx_municipio_estabelecimentos ON estabelecimentos(municipio); 
CREATE INDEX idx_descricao_municipio ON municipios(descricao); 


SELECT 
	 em.cnpj_base
	,em.nome
	,es.tipo_logradouro
	,es.logradouro
	,es.numero
	,es.complemento
	,es.ddd1
	,es.telefone1
	,es.correio_eletronico 
	FROM empresas AS em 
INNER JOIN estabelecimentos AS es on es.cnpj_base = em.cnpj_base
INNER JOIN simples AS si on si.cnpj_base = es.cnpj_base 
INNER JOIN municipios AS mu on mu.codigo = es.municipio 
WHERE 
	si.opcao_mei = 'S'
	AND mu.descricao = 'ORTIGUEIRA'
	AND ( es.situacao_cadastral = '02' OR es.situacao_cadastral = '2')
LIMIT 10;
-- 1128 registros

