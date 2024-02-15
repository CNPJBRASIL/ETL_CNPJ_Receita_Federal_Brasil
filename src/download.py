import requests
import threading
import os
from pathlib import Path
import time

BASE_DIR = Path(__file__).resolve().parent.parent

class Downloader:

    def __init__(self, url, folder) -> None:
        #self.dir = os.path.abspath('.\data')
        self.dir = Path.joinpath(BASE_DIR, 'data')
        #self.dir_temp = os.path.abspath(r'.\temp')
        self.dir_temp = Path.joinpath(BASE_DIR, 'temp')
        self.url = url
        self.file_name = self.url.split('/')[-1]
        self.num_threads = 10 # number of threads to use for downloading
        self.folder = folder
        self.path_final = os.path.join(self.dir, self.folder)
        
        self.response = requests.head(url)
        self.file_size = int(self.response.headers.get("Content-Length", 0))
        self.part_size = self.file_size // self.num_threads # size of each part

        threads = []
        for i in range(self.num_threads):
            start = i * self.part_size
            end = (i+1) * self.part_size - 1 if i < self.num_threads - 1 else self.file_size - 1
            t = threading.Thread(target=self.download_part, args=(start, end, i))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        with open(os.path.join(self.path_final, self.file_name), "wb") as f:
            for i in range(self.num_threads):
                part_file = os.path.join(self.dir_temp, f"part{i}.zip")
                with open(part_file, "rb") as part:
                    f.write(part.read())

                while os.path.exists(part_file):
                    os.remove(part_file)

        print(self.file_name, self.file_size)


    def download_part(self, start, end, part_num):
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                headers = {"Range": f"bytes={start}-{end}"}
                r = requests.get(self.url, headers=headers, stream=True, timeout=70)

                with open(os.path.join(self.dir_temp, f"part{part_num}.zip"), "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                break # exit the loop if no error
            except ConnectionResetError:
                retry_count += 1
                print(f"Connection reset by peer. Retrying in 10 seconds. Attempt {retry_count} of {max_retries}.")
                time.sleep(10) # wait for 10 seconds

