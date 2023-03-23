import scrap
import download

scrape = scrap.Scraper()
for url in scrape.urls:
    print(url)
    down = download.Downloader(url)

