import scrap
import download
import data_process
import os
import datetime
from pathlib import Path
now = datetime.datetime.now()

BASE_DIR =  Path(__file__).resolve().parent.parent

class Runner():
    def __init__(self, db, folder_name) -> None:
        self.folder_name = folder_name
       # self.dir = os.path.abspath(r'.\data')
        self.dir = Path.joinpath(BASE_DIR, 'data')
        self.folder_path = os.path.join(self.dir, self.folder_name)
        self.scrape = scrap.Scraper()
        self.urls = [url for url in self.scrape.urls if url.endswith('.zip')]
        self.urls_done = os.listdir(self.folder_path)
        self.urls_todo = [url for url in self.urls if url.split("/")[-1] not in self.urls_done]
        self.db = db

        

    def download(self):
        for url in self.urls_todo:
            down = download.Downloader(url, self.folder_name)
            print(datetime.datetime.now(), url, 'DOWNLOADED')
        
    def process_files(self):
        process = data_process.Process(self.folder_name, type_db=self.db)
        process.files_to_process()
  


if "__main__" == __name__:
    print('START: ', datetime.datetime.now())
    
    folder_name = "zip_2024-01-16"
   # Path.mkdir(os.path.join(BASE_DIR, 'data', folder_name), exist_ok=True)
    type_db= 'sqlserver'
    
    runner = Runner(db=type_db, folder_name=folder_name)
    #runner.download()
    runner.process_files()

    print('END: ', datetime.datetime.now())
