
import pandas as pd
import os
import sqlite3
import time
import re
from zipfile import ZipFile, ZIP_BZIP2
import hashlib

class Model:
    def __init__(self) -> None:

        self.empresas = ['cnpj_base', 'nome', 'natureza_juridica', 'qualificacao', 'capital_social', 'porte', 'ente']
        self.estabelecimentos = ['cnpj_base', 'cnpj_ordem', 'cnpj_dv', 'identificador', 'nome_fantasia', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
        self.simples = ['cnpj_base', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']
        self.socios = ['cnpj_base','identificador_socio','nome_socio','cpf_cnpj','qualificao_socio','data_entrada','pais','representante_legal','nome_representante','qualificacao_representante_legal','faixa_etaria']
        self.paises = ['codigo','descricao']
        self.municipios = ['codigo','descricao']
        self.naturezas = ['codigo','descricao']
        self.cnaes = ['codigo','descricao']
        self.motivos = ['codigo','descricao']
        self.qualificacoes = ['codigo','descricao']
        

        
    def get_columns(self, table_name):
        return self.__dict__.get(table_name)


class File:
    def __init__(self, dir, file_name) -> None:
        self.dir = dir
        self.file_name = file_name
        self.path = f'{self.dir}\{self.file_name}'
        self.table_name  = re.sub('\d+(?=\.zip)', '', self.file_name).replace('%20','_').replace('.zip', '').lower()
        self.columns = Model().get_columns( table_name= self.table_name )
        self.separator = "," if self.columns == None else ";" 
       # verifica se o zip tem  mais de 1 arquivo compactado. Descompacta e compacta cada subarquivo no mesmo diretório
        with ZipFile(f'{self.path}', 'r') as zip:
            self.sub_files = zip.namelist()
            self.qt = len(self.sub_files)

  
    def check_zip(self):
        
        with ZipFile(f'{self.path}', 'r') as zip:
            self.sub_files = zip.namelist()
            self.qt = len(self.sub_files)
        
            if self.qt > 1:
        
                for sub_file in self.sub_files:
                    zip.extract( sub_file, path=f'{self.dir}\\')
                    
                    with ZipFile(f'{self.dir}\{sub_file.replace(".csv","").replace(" ","_",1).replace(" ","")}.zip', 'w', compression=ZIP_BZIP2) as sub_zip:
                        sub_zip.write(f'{self.dir}\{sub_file}')
                    
                    os.remove( f'{self.dir}\{sub_file}')


    def extract_to_df(self):   
       self.df = pd.read_csv(self.path, compression= 'zip', sep= self.separator, low_memory= False, names= self.columns, dtype=str, encoding='ansi', quotechar='"')
       self.df_len = len(self.df)


    def update_df(self):
        self.df['File_Name'] = self.file_name

        def hash(row):
            return hashlib.sha256(row.encode('utf-8')).hexdigest()

        cols = list(self.df.columns)
        self.df['hash'] = self.df[cols].apply(lambda row: hash(''.join(row.values.astype(str))), axis=1)


    def df_to_sql(self, conn):
        self.df.to_sql(self.table_name, conn, if_exists='append', index=False, chunksize=100_000)

    
class Process:
    def __init__(self, folder) -> None:
        self.folder = folder
        self.dir_path = os.path.abspath('.\data')
        self.dir = os.path.join(self.dir_path, self.folder)
        # Unzip sub zip files
        self.files = [File(self.dir, file_name).check_zip() for file_name in os.listdir(self.dir) if file_name.endswith('.zip')]
        # Overwrite self.files to get new subfiles unziped before
        self.files2 = [File(self.dir, file_name) for file_name in os.listdir(self.dir) if file_name.endswith('.zip')]
        
    
    def extract_insert(self):
        self.dir_path_db = os.path.abspath('.\db')
        self.path_db = os.path.join(self.dir_path_db, 'cnpj_db.sqlite')


        self.conn = sqlite3.connect(self.path_db)
        
        for file in self.files2:
           
            if file.qt > 1:
                continue

            file.extract_to_df()
            file.df_to_sql(self.conn)

            print(file.file_name, file.df_len, 'DONE')
            
            # Clear df  to  economy memory
            file.df = None
           
            
        
    