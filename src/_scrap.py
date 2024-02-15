import requests
from bs4 import BeautifulSoup

class Scraper:
    def __init__(self) -> None:
        self.url_cnpj = r'http://200.152.38.155/CNPJ/'
        self.url_tributario = r'http://200.152.38.155/CNPJ/regime_tributario/'
        self.urls = []
    
    def get_html(self):
        self.response_cnpj = requests.get(self.url_cnpj)

    def get_html_urls_cnpj(self):
        self.soup_cnpj = BeautifulSoup(self.response_cnpj.content, 'html.parser').find_all('a')

        for cnpj in self.soup_cnpj:
            name = cnpj.get('href')
            if name.endswith('.zip') | name.endswith('.pdf') :
                self.urls.append(f'{self.url_cnpj}{name}')


    def get_table_html_cnpj(self):
        # Table CNPJ
        self.soup_table = BeautifulSoup(self.response_cnpj.content, 'html.parser').find('table')
        self.rows = self.soup_table.find_all('tr')

        for row in self.rows:
            row_a = row.find('a')
            if row_a:
                file_name = row_a.get('href')
                if file_name.endswith('.zip'):
                    print(f'{self.url_cnpj}{file_name}', row.find_all('td')[2].get_text(), row.find_all('td')[3].get_text() )

