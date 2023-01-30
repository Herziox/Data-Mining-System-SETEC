import sys
import os
import shutil
import pandas as pd
import time
from threading import Thread, Barrier

arr = ['oc','ocf','occ','ocs','oc_cc','oc_cl','pcoc','oec','oecc','oecs','oecf','pcoec','ci','ci_lca','pcci']
doc_name = 'pcci'
QUERY_INPUT = '_'
N_DRIVERS = 4
new_ws = 'no'


os.system(f'python web_scraper.py {doc_name} {QUERY_INPUT} {N_DRIVERS} {new_ws}')

'''
python web_scraper.py pcci _ 4 no 1
'''