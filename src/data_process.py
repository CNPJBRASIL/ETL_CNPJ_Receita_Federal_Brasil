
import pandas as pd

class Model:
    def __init__(self) -> None:

        self.empresas = ['cnpj_base', 'nome', 'natureza_juridica', 'qualificacao', 'capital_social', 'porte', 'ente']

        self.estabelecimentos = ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'identificador', 'nome_fantasia', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']

        self.simples = ['cnpj_base', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']

        self.socios = ['cnpj_base','identificador_socio','nome_socio','cpf_cnpj','qualificao_socio','data_entrada','pais','representante_legal','nome_representante','qualificacao_representante_legal','faixa_etaria']

        self.paises = ['codigo','descricao']

        self.municipios = ['codigo','descricao']

        self.natureza_juridica = ['codigo','descricao']

        self.cnae = ['codigo','descricao']
        


class Check:
    def __init__(self) -> None:
        self.model = Model()
        self.col = self.model.empresas
        self.file = r"D:\CNPJ\zip_2023-03-15\Empresas0.zip"
        self.df = pd.read_csv(self.file, compression='zip', sep=';',low_memory=False, nrows=5,names=self.col)