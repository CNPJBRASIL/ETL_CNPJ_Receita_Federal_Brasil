
import pandas as pd
import os
import sqlite3
import datetime
import re
from zipfile import ZipFile, ZIP_BZIP2
import hashlib
import env.db
import sqlalchemy 
from sqlalchemy.engine import URL
from pathlib import Path
from tqdm import tqdm
import pyodbc
import concurrent.futures

BASE_DIR = Path(__file__).resolve().parent.parent

class Model:

    def __init__(self) -> None:

        self.empresas = ['cnpj_base', 'nome', 'natureza_juridica', 'qualificacao', 'capital_social', 'porte', 'ente']
        self.estabelecimentos = ['cnpj_base', 
                            'cnpj_ordem', 
                            'cnpj_dv', 
                            'identificador', 
                            'nome_fantasia', 
                            'situacao_cadastral',
                            'data_situacao_cadastral', 
                            'motivo_situacao_cadastral', 
                            'cidade_exterior', 
                            'pais', 
                            'data_inicio_atividade', 
                            'cnae_fiscal_principal', 
                            'cnae_fiscal_secundaria', 
                            'tipo_logradouro', 
                            'logradouro', 
                            'numero', 
                            'complemento', 
                            'bairro', 
                            'cep', 
                            'uf', 
                            'municipio', 
                            'ddd1', 
                            'telefone1', 
                            'ddd2', 
                            'telefone2', 
                            'ddd_fax', 
                            'fax', 
                            'correio_eletronico', 
                            'situacao_especial', 
                            'data_situacao_especial']
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
        # self.path = f'{self.dir}\{self.file_name}'
        self.path = os.path.join(self.dir, self.file_name)
        self.table_name  = re.sub(r'\d+(?=\.zip)', '', self.file_name).replace('%20','_').replace('.zip', '').lower()
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
                    
                    #with ZipFile(f'{self.dir}\{sub_file.replace(".csv","").replace(" ","_",1).replace(" ","")}.zip', 'w', compression=ZIP_BZIP2) as sub_zip:
                    
                    with ZipFile(os.path.join( self.dir, f'{sub_file.replace(".csv","").replace(" ","_",1).replace(" ","")}.zip'), 'w', compression=ZIP_BZIP2) as sub_zip:
                        sub_zip.write(os.path.join(self.dir, sub_file) )
                    
                    # os.remove( f'{self.dir}\{sub_file}')
                    os.remove(os.path.join(self.dir, sub_file))


class Chunck:
    def __init__(self, chunck, db, file_name, table_name) -> None:
        self.df = chunck
        self.db = db
        self.file_name = file_name
        self.table_name = table_name

    
    def transform(self):
        self.df['File_Name'] = self.file_name
       
        def hash(row):
            return hashlib.sha256(row.encode('utf-8')).hexdigest()

        cols = list(self.df.columns)
        self.df['hash'] = self.df[cols].apply(lambda row: hash(''.join(row.values.astype(str))), axis=1)


    def insert_db(self):
        self.df.to_sql(self.table_name, self.db.conn, if_exists='append', index=False, chunksize=100_000)
        self.db.conn.close()


    def process(self):
        #self.transform()
        self.insert_db()
        
    
class Db:
    def __init__(self, type_db) -> None:
        if type_db == 'sqlserver':      
            self.server = env.db.HOST
            self.database = env.db.DATABASE
            self.driver = '{ODBC Driver 17 for SQL Server}'
            self.cnxn = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={env.db.USER};PWD={env.db.PASS}"
            # https://docs.sqlalchemy.org/en/20/dialects/mssql.html#pass-through-exact-pyodbc-string
            #self.connection_string = "DRIVER={SQL Server Native Client 10.0};SERVER=dagger;DATABASE=test;UID=user;PWD=password"
            self.connection_string = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={env.db.USER};PWD={env.db.PASS}"
            self.connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.connection_string})
            self.engine = sqlalchemy.create_engine(self.connection_url, fast_executemany=True)
            self.conn = self.engine.connect()

        if type_db == 'sqlite':
            # self.dir_path_db = os.path.abspath('.\db')
            self.dir_path_db = os.path.join(BASE_DIR, 'db')
            self.path_db = os.path.join(self.dir_path_db, 'cnpj_db.sqlite')
            self.conn = sqlite3.connect(self.path_db)


class Db2:
    def __init__(self) -> None:
        self.DRIVER = 'ODBC Driver 17 for SQL Server'
        self.SERVER = os.getenv('HOST')
        self.DATABASE = os.getenv('DATABASE')
        self.UID = os.getenv('USER')
        self.PWD = os.getenv('PASS')
        self.string_connection =  f"DRIVER={self.DRIVER};SERVER={self.SERVER};DATABASE={self.DATABASE};UID={self.UID};PWD={self.PWD}"



class Process:
    def __init__(self, folder, type_db) -> None:
        self.folder = folder
        self.type_db = type_db
        #self.dir_path = os.path.abspath('.\data')
        self.dir_path = os.path.join(BASE_DIR, 'data')
        self.dir = os.path.join(self.dir_path, self.folder)
        # Unzip sub zip files
        self.files = [File(self.dir, file_name).check_zip() for file_name in os.listdir(self.dir) if file_name.endswith('.zip')]
        # Overwrite self.files to get new subfiles unziped before
        self.files2 = [File(self.dir, file_name) for file_name in os.listdir(self.dir) if file_name.endswith('.zip')]
        
        # with open('D:\OneDrive\Dev\ETL_CNPJ_Receita_Federal_Brasil\src\done.txt', 'r') as f:
        with open( os.path.join(BASE_DIR, 'src', 'done.txt') ) as f:
            self.processed_files = [line.strip() for line in f.readlines()]
            

    def files_to_process(self):
        for file in self.files2:
            
            print(datetime.datetime.now(), file.file_name, 'START')

            if file.qt > 1 or file.file_name in self.processed_files: 
                print(file.file_name, 'ALREADY DONE')
                continue

            # Le o csv do zip inteiro e particiona em chuncks conforme chuncksize
            with pd.read_csv(file.path, compression= 'zip', sep= file.separator, low_memory= False, names= file.columns, dtype=str, encoding='ansi', quotechar='"', chunksize= 3_000_000) as reader:
                
                #for chunck in tqdm(reader,desc='Processing chunks', unit='chunk'):
                #    db = Db(self.type_db)
                #    chunck_obj = Chunck(chunck, db, file.file_name, file.table_name)
                #    chunck_obj.process()


                def call_chunck(chunck, file_name, table_name):
                    db = Db(self.type_db)
                    chunck_obj = Chunck(chunck, db, file_name, table_name)
                    chunck_obj.process()
                    print('Chunck processado ...')

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(call_chunck, chunk, file.file_name, file.table_name) for chunk in reader]

                for future in concurrent.futures.as_completed(futures):
                    print('Future result: ', future.result(), file.file_name)

           
            with open('done.txt', 'a') as f:
                f.write(f'\n{file.file_name}')

                

            print(datetime.datetime.now(), file.file_name, 'DONE')         
           
            
        
    