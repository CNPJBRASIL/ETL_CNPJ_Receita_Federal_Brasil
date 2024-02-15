from pathlib import Path
import os
import logging

logging.basicConfig(level=logging.DEBUG)

BASE_DIR = Path(__file__).resolve().parent.parent

print(BASE_DIR)

datapath = Path.joinpath(BASE_DIR, 'data')
print(datapath)

print(os.path.abspath('.\db'))

print(Path.joinpath(BASE_DIR, 'src', 'done.txt') )




# docker run -it -d --name cnpjcoletacontainer -v data-volume:/data cnpjcoletaimage
# docker build -t cnpjcoletaimage .


def test_log1():
    a = 5
    logging.info(f'O a vale {a}')

test_log1()


print(os.getenv('HOST'))