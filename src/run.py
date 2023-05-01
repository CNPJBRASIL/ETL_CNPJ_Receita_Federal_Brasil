import scrap
import download
import data_process
import os
import datetime
now = datetime.datetime.now()



class Runner():
    def __init__(self) -> None:
        self.folder_name = "zip_2023-04-11"
        self.dir = os.path.abspath('.\data')
        self.folder_path = os.path.join(self.dir, self.folder_name)
        self.scrape = scrap.Scraper()
        self.urls = [url for url in self.scrape.urls if url.endswith('.zip')]
        self.urls_done = os.listdir(self.folder_path)
        self.urls_todo = [url for url in self.urls if url.split("/")[-1] not in self.urls_done]

    def download(self):
        for url in self.urls_todo:
            down = download.Downloader(url, self.folder_name)
            print(url, 'DOWNLOADED')
        
    def process_files(self):
        process = data_process.Process(self.folder_name)
        process.extract_insert()


if "__main__" == __name__:
    print('START: ', datetime.datetime.now())

    runner = Runner()
    #runner.download()
    runner.process_files()

    print('END: ', datetime.datetime.now())