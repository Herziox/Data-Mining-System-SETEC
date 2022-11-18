# Importación de librerías
import pandas as pd
import numpy as np
import os
import re

# Ruta de datos
base_path = os.getcwd()
path = os.path.join(base_path, "CLEANED_DATA")

# Datamart OC

## Dimensión Razon Social OC

## Dimensión Curso_Perfil

## Dimensión Fecha
df_pcoc = pd.read_csv(os.path.join(path,'pcoc.csv'))
year_list = [str(x) for x in range(2022,2009,-1)]

def get_year(x):
    for year in year_list:
        if re.search('-'+year+'\s|'+year+'-|-'+year+'$',x):
            return year
    return 'sin_anio'

df_pcoc['anio'] = df_pcoc['Numero_Certificado'].apply(get_year)

df_oc = pd.read_csv(os.path.join(path,'oc.csv'))

df_extract_0_oc = df_oc[['Razon_Social','Fecha_Resolucion']]

df_pcoc_1 = pd.merge(df_pcoc,df_extract_0_oc,left_on=['Razon_Social_OC'],right_on=['Razon_Social'],how="left") 



## Hechos OC




