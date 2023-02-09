import sys
import os
import shutil
import pandas as pd
import time
from threading import Thread, Barrier

arr = ['oc','ocf','occ','ocs','oc_cc','oc_cl','pcoc','oec','oecc','oecs','oecf','pcoec','ci','ci_lca','pcci'] # Lista de submodulos a scrapear
doc_name = 'pcci' # Nombre del submodulo que quiero extraer los datos y que sera el nombre del documento CSV
QUERY_INPUT = '_' # Consulta que quiero realizar en el cuadro de texto
N_DRIVERS = 4 # Numero de controladores y particiones que quiero hacer para llamar a multiples Scraper
new_ws = 'no' # Permite borrar los metadatos que recolectan informacion hacerca de si se quiere o no extraer datos nuevos

### Se envia la ejecucion del archivo web_scraper con los paremetros respectivos
os.system(f'python web_scraper.py {doc_name} {QUERY_INPUT} {N_DRIVERS} {new_ws}')

'''
python web_scraper.py pcci _ 4 no 1
'''