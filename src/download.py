import requests
import threading
import os

class Downloader:

    def __init__(self, url, folder) -> None:
        self.dir = os.path.abspath('.\data')
        self.dir_temp = os.path.abspath(r'.\temp')
        self.url = url
        self.file_name = self.url.split('/')[-1]
        self.num_threads = 10 # number of threads to use for downloading
        self.folder = folder
        self.path_final = os.path.join(self.dir, self.folder)
        print(self.path_final)
        
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

                os.remove(part_file)

        print(self.file_name, self.file_size)


    def download_part(self, start, end, part_num):
        headers = {"Range": f"bytes={start}-{end}"}
        r = requests.get(self.url, headers=headers, stream=True)

        with open(os.path.join(self.dir_temp, f"part{part_num}.zip"), "wb") as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)