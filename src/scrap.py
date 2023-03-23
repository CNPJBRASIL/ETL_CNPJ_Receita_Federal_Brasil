import requests
from bs4 import BeautifulSoup


class Scraper:
    def __init__(self) -> None:
        self.url_cnpj = r'https://dados.rfb.gov.br/CNPJ/'
        self.url_tributario = r'https://dados.rfb.gov.br/CNPJ/regime_tributario/'
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
