import scrap
import download
import data_process
import os



class Runner():
    def __init__(self) -> None:
        self.folder_name = "zip_2023-04-11"
        self.dir = os.path.abspath('.\data')
        self.folder_path = os.path.join(self.dir, self.folder_name)
        self.scrape = scrap.Scraper()
        self.urls = self.scrape.urls
        self.urls_done = os.listdir(self.folder_path)
        self.urls_todo = [url for url in self.urls if url.split("/")[-1] not in self.urls_done]

        for url in self.urls_todo:
            down = download.Downloader(url, self.folder_name)
            print(url, 'DOWNLOADED')
        
        process = data_process.Process(self.folder_name)
        process.extract_insert()


if "__main__" == __name__:
    runner = Runner()
