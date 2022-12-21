#!/usr/bin/env python
# coding: utf-8

# # Importación de librerías

# In[2]:


import numpy as np
import pandas as pd
import os
import mysql.connector
from tqdm import tqdm


# # Ruta de archivos

# In[3]:


path_dwh =  os.path.join(os.getcwd(),'DATAWAREHOUSE')
path_dmoc = os.path.join(path_dwh,'datamart_oc')
path_dmoec = os.path.join(path_dwh,'datamart_oec')
path_dmci = os.path.join(path_dwh,'datamart_ci')


# # Conexión con MySQL

# In[4]:


mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    passwd='KappaTao1233'
)

mycursor = mydb.cursor()


# # Creación de la base de datos

# In[22]:


mycursor.execute("DROP DATABASE IF EXISTS dwh_setec")
mycursor.execute("CREATE DATABASE dwh_setec")


# In[5]:


mycursor.execute("USE dwh_setec")


# # DATAMART OC

# In[11]:


os.listdir(path_dmoc)


# ## Tabla dim_razon_social_oc

# In[3]:


df_oc = pd.read_csv(os.path.join(path_dmoc,'dim_razon_social_oc.csv'))
df_oc['id_oc'] = df_oc['id_oc'].astype(str)


# In[23]:


mycursor.execute("DROP TABLE IF EXISTS dim_razon_social_oc")
mycursor.execute('''CREATE TABLE dim_razon_social_oc (
id_oc VARCHAR(4) PRIMARY KEY,
razon_social VARCHAR(255) NOT NULL,
estado VARCHAR(63) NOT NULL,
canton VARCHAR(63) NOT NULL,
provincia VARCHAR(63) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_razon_social_oc ( id_oc, razon_social, estado, canton, provincia) VALUES (%s,%s,%s,%s,%s)'
for row in tqdm(df_oc.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_curso_perfil_oc

# In[19]:


df_cur_per = pd.read_csv(os.path.join(path_dmoc,'dim_curso_perfil.csv'))
df_cur_per['id_curso_perfil'] = df_cur_per['id_curso_perfil'].astype(str)
df_cur_per = df_cur_per.fillna({'costo':0})


# In[58]:


mycursor.execute("DROP TABLE IF EXISTS dim_curso_perfil_oc")
mycursor.execute('''CREATE TABLE dim_curso_perfil_oc (
id_curso_perfil VARCHAR(4) PRIMARY KEY,
tipo VARCHAR(63) NOT NULL,
area_familia VARCHAR(255) NOT NULL,
especialidad_sector VARCHAR(255) NOT NULL,
curso_perfil VARCHAR(255) NOT NULL,
modalidad VARCHAR(255) NOT NULL,
carga_horaria FLOAT NOT NULL,
costo FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO dim_curso_perfil_oc ( id_curso_perfil, tipo, area_familia, especialidad_sector, curso_perfil, modalidad, carga_horaria, costo) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_cur_per.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_fecha_oc

# In[59]:


df_fecha_oc = pd.read_csv(os.path.join(path_dmoc,'dim_fecha.csv'))
df_fecha_oc['id_fecha'] = df_fecha_oc['id_fecha'].astype(str)
df_fecha_oc['anio'] = df_fecha_oc['anio'].astype(str)


# In[60]:


mycursor.execute("DROP TABLE IF EXISTS dim_fecha_oc")
mycursor.execute('''CREATE TABLE dim_fecha_oc (
id_fecha VARCHAR(4) PRIMARY KEY,
anio VARCHAR(4) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_fecha_oc ( id_fecha, anio) VALUES (%s,%s)'
for row in tqdm(df_fecha_oc.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla fact_oc

# In[61]:


df_fact_oc = pd.read_csv(os.path.join(path_dmoc,'fact_oc.csv'))
df_fact_oc['id_oc'] = df_fact_oc['id_oc'].astype(str)
df_fact_oc['id_curso_perfil'] = df_fact_oc['id_curso_perfil'].astype(str)
df_fact_oc['id_fecha'] = df_fact_oc['id_fecha'].astype(str)


# In[57]:


mycursor.execute("DROP TABLE IF EXISTS fact_oc")
mycursor.execute('''CREATE TABLE fact_oc (
id_oc VARCHAR(4) NOT NULL,
id_curso_perfil VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cap_cer INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_horas INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = '''INSERT INTO fact_oc (id_oc, id_curso_perfil, id_fecha, num_cap_cer, total_cursos, volumen_capacitados, total_horas, total_ganancias) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
for row in tqdm(df_fact_oc.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## CREAR CLAVES FORANEAS OC

# In[68]:


mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_oc(id_fecha)
''')
mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_oc) REFERENCES dim_razon_social_oc(id_oc)
''')
mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_curso_perfil) REFERENCES dim_curso_perfil_oc(id_curso_perfil)
''')


# # DATAMART OEC

# In[71]:


os.listdir(path_dmoec)


# ## Tabla dim_razon_social_oec

# In[76]:


df_oec = pd.read_csv(os.path.join(path_dmoec,'dim_razon_social_oec.csv'))
df_oec['id_oec'] = df_oec['id_oec'].astype(str)


# In[80]:


mycursor.execute("DROP TABLE IF EXISTS dim_razon_social_oec")
mycursor.execute('''CREATE TABLE dim_razon_social_oec (
id_oec VARCHAR(4) PRIMARY KEY,
razon_social VARCHAR(255) NOT NULL,
estado VARCHAR(63) NOT NULL,
canton VARCHAR(63) NOT NULL,
provincia VARCHAR(63) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_razon_social_oec ( id_oec, razon_social, estado, canton, provincia) VALUES (%s,%s,%s,%s,%s)'
for row in tqdm(df_oec.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_perfil

# In[91]:


df_perfil = pd.read_csv(os.path.join(path_dmoec,'dim_perfil.csv'))
df_perfil['id_perfil'] = df_perfil['id_perfil'].astype(str)


# In[98]:


mycursor.execute("DROP TABLE IF EXISTS dim_perfil_oec")
mycursor.execute('''CREATE TABLE dim_perfil_oec (
id_perfil VARCHAR(4) PRIMARY KEY,
familia VARCHAR(255) NOT NULL,
sector VARCHAR(255) NOT NULL,
perfil VARCHAR(255) NOT NULL,
modalidad VARCHAR(63) NOT NULL,
carga_horaria INT NOT NULL,
costo FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO dim_perfil_oec (id_perfil, familia, sector, perfil, modalidad, carga_horaria, costo) VALUES (%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_perfil.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_fecha_oec

# In[114]:


df_fecha_oec = pd.read_csv(os.path.join(path_dmoec,'dim_fecha.csv'))
df_fecha_oec['id_fecha'] = df_fecha_oec['id_fecha'].astype(str)


# In[116]:


mycursor.execute("DROP TABLE IF EXISTS dim_fecha_oec")
mycursor.execute('''CREATE TABLE dim_fecha_oec (
id_fecha VARCHAR(4) PRIMARY KEY,
dia INT NOT NULL,
mes INT NOT NULL,
anio INT NOT NULL
)''')
sql_formula = 'INSERT INTO dim_fecha_oec (id_fecha, dia, mes, anio) VALUES (%s,%s,%s,%s)'
for row in tqdm(df_fecha_oec.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla fact_oec

# In[52]:


df_fact_oec = pd.read_csv(os.path.join(path_dmoec,'fact_oec.csv'))
df_fact_oec['id_oec'] = df_fact_oec['id_oec'].astype(str)
df_fact_oec['id_perfil'] = df_fact_oec['id_perfil'].astype(str)
df_fact_oec['id_fecha'] = df_fact_oec['id_fecha'].astype(str)


# In[53]:


mycursor.execute("DROP TABLE IF EXISTS fact_oec")
mycursor.execute('''CREATE TABLE fact_oec (
id_oec VARCHAR(4) NOT NULL,
id_perfil VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cer INT NOT NULL,
total_horas INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO fact_oec(id_oec, id_perfil, id_fecha, num_cer, total_horas, total_cursos, volumen_capacitados, total_ganancias) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_fact_oec.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## CREAR CLAVES FORANEAS OEC

# In[57]:


mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_oec(id_fecha)
''')
mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_oec) REFERENCES dim_razon_social_oec(id_oec)
''')
mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_perfil) REFERENCES dim_perfil_oec(id_perfil)
''')


# # DATAMART CI

# In[127]:


os.listdir(path_dmci)


# ## Tabla dim_razon_social_ci

# In[7]:


df_ci = pd.read_csv(os.path.join(path_dmci,'dim_razon_social_ci.csv'))
df_ci['id_ci'] = df_ci['id_ci'].astype(str)


# In[8]:


mycursor.execute("DROP TABLE IF EXISTS dim_razon_social_ci")
mycursor.execute('''CREATE TABLE dim_razon_social_ci (
id_ci VARCHAR(4) PRIMARY KEY,
razon_social VARCHAR(255) NOT NULL,
codigo_resolucion VARCHAR(255) NOT NULL,
provincia VARCHAR(63) NOT NULL,
canton VARCHAR(63) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_razon_social_ci( id_ci, razon_social, codigo_resolucion, provincia, canton) VALUES (%s,%s,%s,%s,%s)'
for row in tqdm(df_ci.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_curso

# In[9]:


df_curso = pd.read_csv(os.path.join(path_dmci,'dim_curso_ci.csv'))
df_curso['id_curso'] = df_curso['id_curso'].astype(str)
df_curso = df_curso.fillna({'costo':0})
df_curso = df_curso.dropna()


# In[10]:


mycursor.execute("DROP TABLE IF EXISTS dim_curso_ci")
mycursor.execute('''CREATE TABLE dim_curso_ci (
id_curso VARCHAR(4) PRIMARY KEY,
curso VARCHAR(255) NOT NULL,
area VARCHAR(255) NOT NULL,
especialidad VARCHAR(255) NOT NULL,
modalidad VARCHAR(63) NOT NULL,
carga_horaria INT NOT NULL,
costo FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO dim_curso_ci (id_curso, curso, area, especialidad, modalidad, carga_horaria, costo) VALUES (%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_curso.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla dim_fecha_ci

# In[13]:


df_fecha_ci = pd.read_csv(os.path.join(path_dmci,'dim_fecha_ci.csv'))
df_fecha_ci['id_fecha'] = df_fecha_ci['id_fecha'].astype(str)


# In[14]:


mycursor.execute("DROP TABLE IF EXISTS dim_fecha_ci")
mycursor.execute('''CREATE TABLE dim_fecha_ci (
id_fecha VARCHAR(4) PRIMARY KEY,
anio VARCHAR(15) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_fecha_ci (id_fecha, anio) VALUES (%s,%s)'
for row in tqdm(df_fecha_ci.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## Tabla fact_ci

# In[15]:


df_fact_ci = pd.read_csv(os.path.join(path_dmci,'fact_ci.csv'))
df_fact_ci['id_ci'] = df_fact_ci['id_ci'].astype(str)
df_fact_ci['id_curso'] = df_fact_ci['id_curso'].astype(str)
df_fact_ci['id_fecha'] = df_fact_ci['id_fecha'].astype(str)


# In[16]:


mycursor.execute("DROP TABLE IF EXISTS fact_ci")
mycursor.execute('''CREATE TABLE fact_ci (
id_ci VARCHAR(4) NOT NULL,
id_curso VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cap INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_horas INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO fact_ci(id_ci, id_curso, id_fecha, num_cap, total_cursos, volumen_capacitados, total_horas, total_ganancias) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_fact_ci.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# In[17]:


df_ci.info()
# ', '.join(df_fact_ci.columns)


# ## CREAR CLAVES FORANEAS CI

# In[18]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_ci(id_fecha)
''')


# In[19]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_ci) REFERENCES dim_razon_social_ci(id_ci)
''')


# In[20]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_curso) REFERENCES dim_curso_ci(id_curso)
''')

