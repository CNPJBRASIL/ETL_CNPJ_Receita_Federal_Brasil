
import pandas as pd
import os
import sqlite3
import time
import re
from zipfile import ZipFile

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
        
    def get(self, file_name):
        table  = re.sub('\d+(?=\.zip)', '', file_name).replace('%20','_').replace('.zip', '').lower()
        
        return {'table':table, 'columns':self.__dict__.get(table)}


    def get_model(self, file_name):
        category = ''
        if file_name.startswith("Empresas"):
            category = 'empresas'
        elif file_name.startswith("Estabelecimentos"):
            category = 'estabelecimentos'
        elif file_name.startswith('Simples'):
            category = 'simples'
        elif file_name.startswith('Socios'):
            category = 'socios'
        elif file_name.startswith('Paises'):
            category = 'paises'
        elif file_name.startswith('Municipios'):
            category = 'municipios'
        elif file_name.startswith('Naturezas'):
            category = 'natureza_juridica'
        elif file_name.startswith('Cnae'):
            category = 'cnae'
        

        return {'table':category, 'columns':self.__dict__[category]}
    

class File:
    def __init__(self, dir, file_name) -> None:
        self.dir = dir
        self.file_name = file_name
        self.path = f'{self.dir}\{self.file_name}'
        self.model = Model().get(file_name=self.file_name)
        self.table = self.model['table']
        self.columns = self.model['columns']
        self.df = ''
       

    def extract_to_df(self):
       if self.columns == None:
           separator = ','
       else:
           separator = ';'
    
       self.df = pd.read_csv(self.path, compression= 'zip', sep= separator, low_memory= False, names= self.columns, dtype=str, encoding='ansi')

    
class Process:
    def __init__(self, dir) -> None:
        self.dir = dir
        self.files = [f for f in os.listdir(self.dir) if f.endswith('.zip')]
        self.path_db = r'D:\OneDrive\Dev\ETL_CNPJ_Receita_Federal_Brasil\data\cnpj_db.sqlite' 
        self.conn = sqlite3.connect(self.path_db)

        _start_time = time.time()
        print('Processo iniciado')

        for file in self.files:
            _start_time = time.time()

            file_obj = File(dir=self.dir, file_name=file)
            file_obj.extract_to_df()
            file_obj.df.to_sql(file_obj.table, self.conn, if_exists='append', index=False, chunksize=100_000)
            
            _end_time = time.time();
            _elapsed_time = _end_time - _start_time
            _hours, _remainder = divmod(_elapsed_time, 3600)
            _minutes, _seconds = divmod(_remainder, 60)
            print(f"Arquivo {file} processado em {int(_hours)}H:{int(_minutes)}M:{_seconds:.2f}S")

        _end_time = time.time();
        _elapsed_time = _end_time - _start_time
        _hours, _remainder = divmod(_elapsed_time, 3600)
        _minutes, _seconds = divmod(_remainder, 60)
        print(f"Processo demorou: {int(_hours)}H:{int(_minutes)}M:{_seconds:.2f}S")




# only check file in coding
class Check:
    def __init__(self) -> None:
        self.model = Model()
        self.col = self.model.empresas
        self.file = r"D:\CNPJ\zip_2023-03-15\Empresas0.zip"
        self.dir = r"D:\CNPJ\zip_2023-03-15"

        self.files = os.listdir(self.dir)

        print(self.files)

        for file in self.files:
            category = ''
            if file.startswith("Empresas"):
                category = 'empresas'
            elif file.startswith("Estabelecimentos"):
                category = 'estabelecimentos'
            elif file.startswith('Simples'):
                category = 'simples'
            elif file.startswith('Socios'):
                category = 'socios'
            elif file.startswith('Paises'):
                category = 'paises'
            elif file.startswith('Municipios'):
                category = 'municipios'
            elif file.startswith('Naturezas'):
                category = 'natureza_juridica'
            elif file.startswith('Cnae'):
                category = 'cnae'

            
            if category:
                path = f'{self.dir}\{file}'
                try:


                    df = pd.read_csv(path, compression='zip', sep=';',low_memory=False, nrows=3,names=self.model.__dict__[category], dtype=str, encoding='ansi')
                    print(file)
                    print(df)

                except Exception as error:
                    print(file, error)
                    

if "__main__" == __name__:
    #check = Check()

    model = Model()
    print(model.get_model('Empresas01'))