
import os
import threading
import requests


class Downloader:

    def __init__(self, url) -> None:
        
        self.url = url
        self.file_name = self.url.split('/')[-1]
        self.num_threads = 10 #number of threads to use for downloading
        self.path_final = r"D:\CNPJ\zip_2023-03-15"

        self.response = requests.head(url)
        self.file_size = int(self.response.headers.get("Content-Length", 0))
        self.part_size = self.file_size // self.num_threads # size of each part

        
        def download_part(start, end, part_num):
            
            headers = {"Range": f"bytes={start}-{end}"}
            r = requests.get(self.url, headers=headers, stream=True)

            with open(f"part{part_num}.zip", "wb") as f:
                for chunck in r.iter_content(chunk_size=1024):
                    if chunck:
                        f.write(chunck)
        
        threads = []
        for i in range(self.num_threads):
            start = i * self.part_size
            end = (i+1) * self.part_size - 1 if 1 < self.num_threads - 1 else self.file_size - 1
            t = threading.Thread(target=download_part, args=(start, end, i))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        with open( f'{self.path_final}\{self.file_name}', "wb") as f:
            for i in range(self.num_threads):
                part_file = f"part{i}.zip"
                with open(part_file, "rb") as part:
                    f.write(part.read())

                os.remove(part_file)

        print(self.file_name, self.file_size)


