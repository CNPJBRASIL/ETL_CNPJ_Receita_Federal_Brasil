import requests
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self) -> None:
        # http://200.152.38.155/CNPJ/
        #self.url_cnpj = r'https://dadosabertos.rfb.gov.br/CNPJ/'
        #self.url_tributario = r'https://dadosabertos.rfb.gov.br/CNPJ/regime_tributario/'
        self.url_cnpj = r'http://200.152.38.155/CNPJ/'
        self.url_tributario = r'http://200.152.38.155/CNPJ/regime_tributario/'
        self.files = []
        self.response_cnpj = requests.get(self.url_cnpj)
        self.response_tributario = requests.get(self.url_tributario)
        
        self.soup_cnpj = BeautifulSoup(self.response_cnpj.content, 'html.parser').find_all('a')
        self.soup_tributario = BeautifulSoup(self.response_tributario.content, 'html.parser').find_all('a')

        for cnpj in self.soup_cnpj:
            name = cnpj.get('href')
            if name.endswith('.zip') | name.endswith('.pdf') :
                self.files.append(f'{self.url_cnpj}{name}')

        for trib in self.soup_tributario:
            name = trib.get('href')
            if name.endswith('.zip') | name.endswith('.pdf') :
                self.files.append(f'{self.url_tributario}{name}')

    @property
    def urls(self):
        return self.files


    def save_cnpj(self):
        # Table CNPJ
        self.soup_table = BeautifulSoup(self.response_cnpj.content, 'html.parser').find('table')
        self.rows = self.soup_table.find_all('tr')

        for row in self.rows:
            row_a = row.find('a')
            if row_a:
                name = row_a.get('href')
                if name.endswith('.zip') | name.endswith('.pdf') :
                    print(f'{self.url_cnpj}{name}', row.find_all('td')[2].get_text(), row.find_all('td')[3].get_text() )
    

    def save_tributario(self):
        # Table Tributa
        self.soup_table = BeautifulSoup(self.response_tributario.content, 'html.parser').find('table')
        self.rows = self.soup_table.find_all('tr')

        for row in self.rows:
            row_a = row.find('a')
            if row_a:
                name = row_a.get('href')
                if name.endswith('.zip') | name.endswith('.pdf') :
                    print(f'{self.url_tributario}{name}', row.find_all('td')[2].get_text(), row.find_all('td')[3].get_text() )
          

