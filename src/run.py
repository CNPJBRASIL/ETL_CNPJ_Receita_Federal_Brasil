import scrap
import download
import data_process

#scrape = scrap.Scraper()
#for url in scrape.urls:
#    print(url)
#    down = download.Downloader(url)

dir = r"D:\CNPJ\zip_2023-03-15"
process = data_process.Process(dir)
process.extract_insert()

