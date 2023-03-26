import scrap
import download
import data_process

#scrape = scrap.Scraper()
#for url in scrape.urls:
#    print(url)
#    down = download.Downloader(url)

check = data_process.Check()
df = check.df
print(df)
