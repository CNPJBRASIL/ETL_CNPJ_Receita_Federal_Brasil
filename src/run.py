import scrap
import download
import data_process


folder = "zip_2023-03-15"

scrape = scrap.Scraper()

for url in scrape.urls:
    print(url)
    down = download.Downloader(url, folder)

process = data_process.Process(folder)
process.extract_insert()

