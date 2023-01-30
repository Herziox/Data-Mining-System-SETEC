#!/usr/bin/env python
# coding: utf-8

# # Importación e Intalación de Librerías

# In[2]:


import csv
import pandas as pd
import sys
import numpy as np
import os
from hermetrics.levenshtein import Levenshtein
import re
import mysql.connector
from tqdm import tqdm


# # Ruta de archivos

# In[3]:


#Rutas de entrada
path_sd = os.path.abspath(os.path.join(os.getcwd(), '..','2_data_understanding_module','collecting_initial_data','existing_data','DIRTY_DATA'))
path_dd = os.path.abspath(os.path.join(os.getcwd(), '..','2_data_understanding_module','collecting_initial_data','existing_data','DOWNLOADED_DATA'))
path_ad = os.path.abspath(os.path.join(os.getcwd(), '..','2_data_understanding_module','collecting_initial_data','additional_data'))

#Rutas salida
path_db = os.path.abspath(os.path.join(os.getcwd(),'CLEANED_DATA'))
path_dwh = os.path.join(os.getcwd(), "DATAWAREHOUSE")
path_dm_oc = os.path.join(path_dwh,'datamart_oc')
path_dm_oec = os.path.join(path_dwh,'datamart_oec')
path_dm_ci = os.path.join(path_dwh,'datamart_ci')


# # Funciones

# In[4]:


stopwords = open(os.path.join(path_db,'spanish.txt'),"r", encoding='utf-8')
stopwords = stopwords.readlines()
stopwords = [x.replace('\n','') for x in stopwords]
global puntuacion
puntuacion = '!"#$%&:;\'()*+,-./?@[\]^_`{|}~()'


# In[5]:


def normalizar(text):
    text = text.lower() 
    text = text.replace('á','a')
    text = text.replace('é','e')
    text = text.replace('í','i')
    text = text.replace('ó','o')
    text = text.replace('ú','u')
    text = text.strip()
    text = text.split(' ')
    re_punc = re.compile('[%s]' % re.escape(puntuacion))
    stripped = [re_punc.sub(' ', w) for w in text]
    text = ' '.join(stripped)
    text = text.split(' ')
    new_text = []
    for i in range(len(text)):
        char = text[i]
        if char not in stopwords and char!=' ':
            new_text.append(text[i])
    new_text = ' '.join(new_text)
    new_text = new_text.strip()
            
    return new_text


# In[6]:


year_list = [str(x) for x in range(2022,2009,-1)]

def get_year(x):
    for year in year_list:
        if re.search('-'+year+'\s|'+year+'-|-'+year+'$',x.replace('/','-')):
            return year
    return 'sin_anio'


# In[7]:


def get_year_by_oc(x,y):
    if x=='sin_anio':
        if y != 'sin fecha resolución' and not pd.isna(y):
            return y[-4:]
    else:
        return x


# In[284]:


lev = Levenshtein()


# # EXTRACCIÓN DE DATOS

# ## Catálogo Nacional de Cualificaciones - CNC 
# 
# - CNC para Operadores de Capacitación - CNCOC
# - CNC para Organismos Evaluadores de  la Conformidad - CNCOEC
# - CNC  Perfiles Inhabilitados - CNCPI

# ### ETL CNCOC

# In[8]:


df_cncoc = pd.read_csv(os.path.join(path_sd,'cncoc.csv'))
df_cncoc.insert(4, 'tipo', "oc")
df_cncoc.insert(5, 'estado', "habilitado")


# In[9]:


# df_cncoc.info()


# ### ETL CNCOEC

# In[10]:


df_cncoec = pd.read_csv(os.path.join(path_sd,'cncoec0.csv'))
df_cncoec.insert(4, 'tipo', "oec")
df_cncoec.insert(5, 'estado', "habilitado")


# In[11]:


# df_cncoec.info()


# ### ETL CNCPI

# In[12]:


df_cncpi = pd.read_csv(os.path.join(path_sd,'cncpi0.csv'))
df_cncpi.insert(1, 'competenecia_laboral', "sin competencia laboral")
df_cncpi.insert(4, 'tipo', "desconocido")
df_cncpi.insert(5, 'estado', "inhabilidato")


# In[13]:


# df_cncpi.info()


#  ### **CNC** - INTEGRACIÓN DE DATOS

# In[14]:


df_cnc = pd.concat([df_cncoc,df_cncoec,df_cncpi],axis=0)
df_cnc = df_cnc.apply(lambda x: x.astype(str).str.lower())


# In[15]:


df_cnc = df_cnc.drop_duplicates()


# In[16]:


#df_cnc.tail()
#df_cnc.info()


# ### Guardar datos CNC

# In[17]:


df_cnc.to_csv(os.path.join(path_db,'cnc.csv'),index=False)


# ## Operadores de Capacitación - OC
# 
# • [Operadores de Capacitación](http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf#j_idt24:j_idt25) - OC
# 
# • [OC - Suspendidos](http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf#j_idt24:j_idt125) - OCS
# 
# • [OC - Finalizaron su vigencia de Calificación](http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf#j_idt24:j_idt134) - OCF
# 
# • [OC - Cancelados](http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf#j_idt24:j_idt144) - OCC
# 

# ### ETL OC

# In[18]:


df_oc = pd.read_csv(os.path.join(path_sd,'oc0.csv'))
df_oc['ruc_o_codigo'] = df_oc['ruc_o_codigo'].astype('object')
df_oc["nombre_comercial"].fillna("Sin Nombre Comercial", inplace = True) 
df_oc.insert(9, 'estado', df_oc.pop('estado'))


# In[19]:


df_oc['ruc_o_codigo'] = df_oc['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)
df_oc['celular'] = df_oc['celular'].apply(lambda x: '0'+x if len(x)==9 else x)


# In[20]:


#df_oc.head()
#df_oc.info()


# ### ETL OCS

# In[21]:


df_ocs = pd.read_csv(os.path.join(path_sd,'ocs.csv'))
df_ocs['ruc_o_codigo'] = df_ocs['ruc_o_codigo'].astype('object')
df_ocs.insert(2, 'nombre_comercial', 'Sin Nombre Comercial')
df_ocs.insert(3, 'telefono', 'Sin Teléfono')
df_ocs.insert(4, 'celular', 'Sin Celular')
df_ocs.insert(5, 'correo_electronico', 'Sin Email')
df_ocs.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_ocs.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_ocs.insert(8, 'canton', 'Sin Cantón')


# In[22]:


#df_ocs.head()
#df_ocs.info()


# ### ETL OCC

# In[23]:


df_occ = pd.read_csv(os.path.join(path_sd,'occ.csv'))
df_occ['ruc_o_codigo'] = df_occ['ruc_o_codigo'].astype('str')
df_occ.insert(2, 'nombre_comercial', 'Sin Nombre Comercial')
df_occ.insert(3, 'telefono', 'Sin Teléfono')
df_occ.insert(4, 'celular', 'Sin Celular')
df_occ.insert(5, 'correo_electronico', 'Sin Email')
df_occ.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_occ.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_occ.insert(8, 'canton', 'Sin Cantón')


# In[24]:


df_occ['ruc_o_codigo'] = df_occ['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)


# In[25]:


#df_occ.head()
#df_occ.info()


# ### ETL OCF

# In[26]:


df_ocf = pd.read_csv(os.path.join(path_sd,'ocf0.csv'))
df_ocf['ruc_o_codigo'] = df_ocf['ruc_o_codigo'].astype('str')
df_ocf.insert(2, 'nombre_comercial', 'Sin Nombre Comercial')
df_ocf.insert(3, 'telefono', 'Sin Teléfono')
df_ocf.insert(4, 'celular', 'Sin Celular')
df_ocf.insert(5, 'correo_electronico', 'Sin Correo Electronico')
df_ocf.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_ocf.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_ocf.insert(8, 'canton', 'Sin Cantón')


# In[27]:


df_ocf['ruc_o_codigo'] = df_ocf['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)


# In[28]:


#df_ocf.head()
#df_ocf.info()


# ### DATOS LIMPIOS OC

# In[29]:


df_oc = pd.concat([df_oc,df_ocs,df_ocf,df_occ],axis=0)
df_oc = df_oc.apply(lambda x: x.astype(str).str.lower())
df_oc = df_oc.drop_duplicates()
df_oc = df_oc.drop_duplicates(subset='razon_social',keep='first')


# In[30]:


#df_oc.head()
#df_OC.info()


# ### Guardar datos OC

# In[31]:


df_oc.to_csv(os.path.join(path_db,'oc.csv'),index=False)


# ### ETL OC_CC

# In[176]:


df_oc_cc = pd.read_csv(os.path.join(path_sd,'oc_cc0.csv'))
df_oc_cc = df_oc_cc.dropna(subset=['area','especialidad','curso','modalidad','carga_horaria'])
df_oc_cc['modalidad'] = df_oc_cc['modalidad'].replace(['VIRTUAL'], 'ONLINE')
df_oc_cc = df_oc_cc.apply(lambda x: x.astype(str).str.lower())
df_oc_cc['carga_horaria'] = df_oc_cc['carga_horaria'].astype('int64')
df_oc_cc = df_oc_cc.drop_duplicates()


# In[177]:


#df_oc_cc.head()
#df_oc_cc.info()


# ### Guardar datos OC_CC

# In[178]:


df_oc_cc.to_csv(os.path.join(path_db,'oc_cc.csv'),index=False)


# ### ETL OC_CL

# In[179]:


df_oc_cl = pd.read_csv(os.path.join(path_sd,'oc_cl0.csv'))
df_oc_cl = df_oc_cl.apply(lambda x: x.astype(str).str.lower())
df_oc_cl = df_oc_cl.drop_duplicates()


# In[180]:


#df_oc_cl.head()
#df_oc_cl.info()


# ### Guardar datos OC_CL

# In[181]:


df_oc_cl.to_csv(os.path.join(path_db,'oc_cl.csv'),index=False)


# ## Personas Capacitadas por OC - PCOC
# 
# • [Búsqueda Personas Capacitadas](http://portal.trabajo.gob.ec/setec-portal-web/pages/personasCapacitadasOperadores.jsf#j_idt24:j_idt25) - PCOC

# In[38]:


df_pcoc = pd.read_csv(os.path.join(path_sd,'pcoc.csv'))
df_pcoc["numero_documento"].fillna("Sin Número Documento", inplace = True)
df_pcoc["apellidos_nombres"].fillna("Sin Apellidos y Nombres", inplace = True)
df_pcoc["numero_horas"].fillna(0, inplace = True)
df_pcoc["numero_horas"]=df_pcoc["numero_horas"].astype('int64')
df_pcoc["razon_social_oc"].fillna("Sin Razón Social OC", inplace = True)
df_pcoc["nombre_comercial_oc"].fillna("Sin Nombre Comercial OC", inplace = True)
df_pcoc = df_pcoc.apply(lambda x: x.astype(str).str.lower())
df_pcoc["numero_horas"]=df_pcoc["numero_horas"].astype('int64')
df_pcoc = df_pcoc.drop_duplicates()    


# In[39]:


#df_pcoc.head()
#df_pcoc.info()


# In[40]:


df_pcoc.to_csv(os.path.join(path_db,'pcoc.csv'),index=False)


# ## Organismos Evaluadores de  la Conformidad - OEC
# 
# • [Organismos Evaluadores de la Conformidad](http://portal.trabajo.gob.ec/setec-portal-web/pages/evaluadoresConformidad.jsf#j_idt24:j_idt25) - OEC
# 
# • [OEC - Suspendidos](http://portal.trabajo.gob.ec/setec-portal-web/pages/evaluadoresConformidad.jsf#j_idt24:j_idt103) - OECS
# 
# • [OEC - Finalizaron su vigencia de Reconocimiento](http://portal.trabajo.gob.ec/setec-portal-web/pages/evaluadoresConformidad.jsf#j_idt24:j_idt112) - OECF
# 
# • [OEC - Cancelados](http://portal.trabajo.gob.ec/setec-portal-web/pages/evaluadoresConformidad.jsf#j_idt24:j_idt122) - OECC

# ### ETL OEC

# In[41]:


df_oec = pd.read_csv(os.path.join(path_sd,'oec0.csv'))
df_oec['celular'] = df_oec['celular'].astype('object')
df_oec = df_oec.apply(lambda x: x.astype(str).str.lower())
df_oec['ruc_o_codigo'] = df_oec['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)
df_oec['celular'] = df_oec['celular'].apply(lambda x: '0'+x if len(x)==9 else x)
df_oec = df_oec.drop_duplicates()


# In[42]:


#df_oec.head()
#df_oec.info()


# ### ETL OECS

# In[43]:


df_oecs = pd.read_csv(os.path.join(path_sd,'oecs.csv'))
df_oecs.insert(2, 'direccion', 'Sin Dirección')
df_oecs.insert(3, 'telefono', 'Sin Teléfono')
df_oecs.insert(4, 'celular', 'Sin Celular')
df_oecs.insert(5, 'correo_electronico', 'Sin Email')
df_oecs.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_oecs.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_oecs.insert(8, 'canton', 'Sin Cantón')
df_oecs = df_oecs.apply(lambda x: x.astype(str).str.lower())


# In[44]:


df_oecs['ruc_o_codigo'] = df_oecs['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)


# In[45]:


#df_oecs.head()
#df_oecs.info()


# ### ETL OECF

# In[46]:


df_oecf = pd.read_csv(os.path.join(path_sd,'oecf0.csv'))
df_oecf.insert(2, 'direccion', 'Sin Dirección')
df_oecf.insert(3, 'telefono', 'Sin Teléfono')
df_oecf.insert(4, 'celular', 'Sin Celular')
df_oecf.insert(5, 'correo_electronico', 'Sin Email')
df_oecf.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_oecf.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_oecf.insert(8, 'canton', 'Sin Cantón')
df_oecf = df_oecf.apply(lambda x: x.astype(str).str.lower())
df_oecs['ruc_o_codigo'] = df_oecs['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)


# In[47]:


#df_oecf.head()
#df_oecf.info()


# ### ETL OECC

# In[48]:


df_oecc = pd.read_csv(os.path.join(path_sd,'oecc.csv'))
df_oecc.insert(2, 'direccion', 'Sin Dirección')
df_oecc.insert(3, 'telefono', 'Sin Teléfono')
df_oecc.insert(4, 'celular', 'Sin Celular')
df_oecc.insert(5, 'correo_electronico', 'Sin Email')
df_oecc.insert(6, 'numero_resolucion', 'Sin Número Resolución')
df_oecc.insert(7, 'fecha_resolucion', 'Sin Fecha Resolución')
df_oecc.insert(8, 'canton', 'Sin Cantón')                               
df_oecc = df_oecc.apply(lambda x: x.astype(str).str.lower())
df_oecc['ruc_o_codigo'] = df_oecc['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)


# In[49]:


#df_oecc.head()
#df_oecc.info()


# ### DATOS LIMPIOS OEC

# In[50]:


df_oec = pd.concat([df_oec,df_oecs,df_oecf,df_oecc],axis=0)
df_oec = df_oec.drop_duplicates()
df_oec = df_oec.drop_duplicates(subset='razon_social',keep='first')


# In[51]:


#df_OEC.head()
#df_OEC.info()


# ### Guardar datos OEC

# In[52]:


df_oec.to_csv(os.path.join(path_db,'oec.csv'),index=False)


# ### ETL OEC_DR

# In[53]:


df_oec_dr = pd.read_csv(os.path.join(path_sd,'oec_dr0.csv'))
df_oec_dr["perfil"].fillna("Sin Perfil", inplace = True)
df_oec_dr["esquema_de_certificacion"].fillna("Sin Esquema de Certificación", inplace = True)
df_oec_dr = df_oec_dr.apply(lambda x: x.astype(str).str.lower())
df_oec_dr['ruc_o_codigo'] = df_oec_dr['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)
df_oec_dr = df_oec_dr.drop_duplicates()


# In[54]:


#df_oec_dr.head()
#df_oec_dr.info()


# ### Guardar datos OEC_DR

# In[55]:


df_oec_dr.to_csv(os.path.join(path_db,'oec_dr.csv'),index=False)


# ## Personas Certificadas por OEC - PCOEC
# 
# • [Búsqueda Personas Certificadas](http://portal.trabajo.gob.ec/setec-portal-web/pages/legitimidadCertificacion.jsf#j_idt24:j_idt25) - PCOEC

# In[56]:


df_pcoec = pd.read_csv(os.path.join(path_sd,'pcoec.csv'))

df_pcoec = df_pcoec.apply(lambda x: x.astype(str).str.lower())
df_pcoec['ruc_o_codigo'] = df_pcoec['ruc_o_codigo'].apply(lambda x: '0'+x if len(x)==12 else x)
df_pcoec['celular'] = df_pcoec['celular'].apply(lambda x: '0'+x if len(x)==9 else x)
df_pcoec = df_pcoec.drop_duplicates()


# In[57]:


#df_pcoec.head()
#df_pcoec.info()


# ### Guardar datos PCOEC

# In[58]:


df_pcoec.to_csv(os.path.join(path_db,'pcoec.csv'),index=False)


# ## Capacitadores Independientes - CI
# 
# • [Búsqueda de Capacitadores Independientes](http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacionIndependientes.jsf#j_idt24:j_idt25) - CI

# ### ETL CI

# In[59]:


df_ci = pd.read_csv(os.path.join(path_sd,'ci0.csv'))
df_ci = df_ci.apply(lambda x: x.astype(str).str.lower())
df_ci['numero_documento'] = df_ci['numero_documento'].apply(lambda x: '0'+x if len(x)==9 else x)
df_ci['celular'] = df_ci['celular'].apply(lambda x: '0'+x if len(x)==9 else x)
df_ci = df_ci.drop_duplicates()


# In[60]:


#df_ci.head()
#df_ci.info()


# ### Guardar datos CI

# In[61]:


df_ci.to_csv(os.path.join(path_db,'ci.csv'),index=False)


# ### ETL CI_LCA

# In[62]:


df_ci_lca =pd.read_csv(os.path.join(path_sd,'ci_lca0.csv'))
df_ci_lca["carga_horaria"].fillna(0, inplace = True)
df_ci_lca["carga_horaria"]=df_ci_lca["carga_horaria"].astype('int64')
df_ci_lca = df_ci_lca.apply(lambda x: x.astype(str).str.lower())
df_ci_lca["carga_horaria"]=df_ci_lca["carga_horaria"].astype('int64')
df_ci_lca = df_ci_lca.drop_duplicates()


# In[63]:


#df_ci_lca.head()
#df_ci_lca.info()


# ### Guardar datos CI_LCA

# In[64]:


df_ci_lca.to_csv(os.path.join(path_db,'ci_lca.csv'),index=False)


# ## Personas Capacitadas por CI - PCCI
# 
# • [Búsqueda de Personas Capacitadas](http://portal.trabajo.gob.ec/setec-portal-web/pages/personasCapacitadas.jsf#j_idt24:j_idt25) - PCCI

# In[65]:


df_pcci =pd.read_csv(os.path.join(path_sd,'pcci.csv'))
df_pcci = df_pcci.apply(lambda x: x.astype(str).str.lower())
df_pcci["numero_horas"]=df_pcci["numero_horas"].astype('int64')
df_pcci = df_pcci.drop_duplicates()


# In[66]:


#df_pcci.head()
#df_pcci.info()


# ### Guardar datos PCCI

# In[67]:


df_pcci.to_csv(os.path.join(path_db,'pcci.csv'),index=False)


# # TRANSFORMACIÓN DE DATOS

#  ## Tabla Ubicacion

# In[68]:


df_ubicacion = pd.read_csv(os.path.join(path_db, 'ubicacion.csv'))


# # DIMENSION Razon Social (OC, OEC y CI)

# In[69]:


df_ci = df_ci.rename(columns={'apellidos_nombres':'razon_social'})


# In[70]:


df_ci = df_ci.rename(columns={'codigo_resolucion':'fecha_resolucion'})


# In[171]:


df_oc_ug = pd.read_csv(os.path.join(path_db,'oc_dl_provincia_canton.csv'))
df_oc_ug['estado']='desconocido'
df_razon_social_oc = pd.concat([df_oc[['razon_social','estado','fecha_resolucion','canton']],df_oc_ug[['razon_social','estado','canton']]],axis=0)
df_razon_social_oc.insert(2,'tipo_razon_social','operador de capacitación')
df_razon_social_oc = df_razon_social_oc.drop_duplicates(subset=['razon_social','canton'],keep='first')
df_oec_ug = pd.read_csv(os.path.join(path_db,'oec_dl_provincia_canton.csv'))
df_oec_ug['estado'] = 'desconocido'
df_razon_social_oec = pd.concat([df_oec[['razon_social','estado','fecha_resolucion','canton']],df_oec_ug[['razon_social','estado','canton']]])
df_razon_social_oec.insert(2,'tipo_razon_social','organismo evaluador de la conformidad')
df_razon_social_oec = df_razon_social_oec.drop_duplicates(subset=['razon_social','canton'], keep='first')
df_ci_ug = pd.read_csv(os.path.join(path_db,'ci_dl_provincia_canton.csv'))
df_razon_social_ci = pd.merge(df_ci,df_ci_ug,on=['razon_social'],how="inner")
df_razon_social_ci.insert(1,'estado','calificado')
df_razon_social_ci = df_razon_social_ci[['razon_social','estado','fecha_resolucion','canton']]
df_razon_social_ci.insert(2,'tipo_razon_social','capacitador independiente')
df_razon_social_ci = df_razon_social_ci.drop_duplicates(subset=['razon_social','canton'],keep='first')
df_razon_social = pd.concat([df_razon_social_oc,df_razon_social_oec,df_razon_social_ci],axis=0)
df_razon_social = pd.merge(df_razon_social,df_ubicacion,on=['canton'],how='left')
df_razon_social.drop(['id_ubicacion'],axis=1,inplace=True)
df_razon_social = df_razon_social.fillna({'provincia':'sin provincia'})
df_razon_social = df_razon_social.fillna({'fecha_resolucion':'sin fecha resolución'})
df_razon_social['estado']=pd.Categorical(df_razon_social['estado'],categories=['calificado', 'habilitado', 'finalizo vigencia de calificacion','finalizo vigencia de reconocimiento','cancelado definitivamente','suspendido','desconocido'])
df_razon_social=df_razon_social.sort_values('estado')
df_razon_social = df_razon_social.drop_duplicates(subset=['razon_social','tipo_razon_social'],keep='first')
df_razon_social=df_razon_social.sort_values(by=['razon_social','tipo_razon_social'])
df_razon_social = df_razon_social.reset_index().rename_axis('id_razon_social').reset_index().astype('str').drop(['index'],axis=1)


# In[174]:


#df_razon_social.head()
#df_razon_social.info()


# # Datamart OC

# ## Dimensión Razon Social OC

# In[182]:


df_razon_social_oc = df_razon_social[df_razon_social['tipo_razon_social']=='operador de capacitación']


# ## Dimensión Curso_Perfil

# In[183]:


df_oc_cl = df_oc_cl.drop(['ruc_o_codigo'],axis=1)
df_oc_cl_2 = pd.read_csv(os.path.join(path_db,'oc_dl_familia_sector_perfil.csv'))
df_oc_cl = pd.concat([df_oc_cl,df_oc_cl_2],axis=0)


# In[184]:


df_oc_cl = df_oc_cl.drop_duplicates()
df_oc_cl['modalidad'] = 'desconocida'
df_oc_cl['carga_horaria'] = 0
df_oc_cl['tipo'] = 'perfil'


# In[185]:


df_oc_cc = df_oc_cc.drop(['documento'],axis=1)
df_oc_cc['tipo'] = 'curso'


# In[186]:


data_curso_perfil = np.concatenate((df_oc_cc.values,df_oc_cl.values), axis=0)
df_curso_perfil = pd.DataFrame(data=data_curso_perfil,columns=['razon_social','area_familia','especialidad_sector','curso_perfil','modalidad','carga_horaria','tipo'])
df_curso_perfil.insert(0,'tipo',df_curso_perfil.pop('tipo'))
df_curso_perfil = df_curso_perfil.rename_axis('id_curso_perfil').reset_index().astype('str')
df_curso_perfil = df_curso_perfil.apply(lambda x: x.astype(str).str.lower())
df_curso_perfil['carga_horaria'] = df_curso_perfil['carga_horaria'].astype('int64')


# In[187]:


#df_curso_perfil.head()
#df_curso_perfil.info()


# ## Hechos OC

# In[188]:


#AGREGAR AÑO
# Cargar los archivos necesarios de la ubicación de CLEANED DATA

# Se extraera el año de la columna Numero_certificado.
# Para esto se buscará porciones de texto que contengan los años del 2000 - 2022 (año actual)

df_pcoc['anio'] = df_pcoc['numero_certificado'].apply(get_year)

df_pcoc = pd.merge(df_pcoc,df_razon_social_oc[['razon_social','fecha_resolucion']],left_on=['razon_social_oc'],right_on=['razon_social'],how="left") 


df_pcoc['anio'] = df_pcoc.apply(lambda x: get_year_by_oc(x['anio'], x['fecha_resolucion']), axis=1)

df_pcoc[
    (df_pcoc['anio']=='sin_anio') & 
    (df_pcoc['fecha_resolucion']!='sin fecha resolución') &
    (df_pcoc['fecha_resolucion'].notnull())
    ]['anio'] = df_pcoc[
    (df_pcoc['anio']=='sin_anio') & 
    (df_pcoc['fecha_resolucion']!='sin fecha resolución') &
    (df_pcoc['fecha_resolucion'].notnull())
    ]['fecha_resolucion'].str.slice(start=-4)

df_pcoc = df_pcoc.drop(['razon_social','fecha_resolucion'],axis=1)


# ## Dimension Fecha OC
# 
# Se debe desarrollar en base a el año el cual se haya obtenido el certificado del curso o perfil OC.
# 
# En caso de no existir un año registrado en el certificado en la tabla **PCOC** se deberá tomar como referencia el año de resolución de la Razon social de la tabla **OC**

# In[189]:


df_fecha_oc = pd.DataFrame(data=df_pcoc['anio'].sort_values().unique(),columns=['anio'])
df_fecha_oc = df_fecha_oc.rename_axis('id_fecha').reset_index().astype('str')
df_fecha_oc = df_fecha_oc[:-1]


# In[190]:


df_fecha_oc.to_csv(os.path.join(path_dm_oc,'dim_fecha.csv'),index=False)


# ## Guardar Datamart OC

# In[191]:


df_fact_oc = df_pcoc.groupby(['razon_social_oc','nombre_curso_perfil','anio']).count().reset_index().iloc[:,:4]


# In[193]:


## UNIR DATOS
df_fact_oc = df_pcoc.groupby(['razon_social_oc','nombre_curso_perfil','anio']).count().reset_index().iloc[:,:4]
df_fact_oc = df_fact_oc.rename(columns={'numero_documento':'num_cap_cer'})
df_total_cap_oc = df_pcoc.groupby(['razon_social_oc','anio']).count().reset_index().iloc[:,:3]
df_total_cap_oc = df_total_cap_oc.rename(columns={'numero_documento':'volumen_capacitados'})
df_total_cur_oc = df_pcoc.groupby(['razon_social_oc','nombre_curso_perfil','anio']).count().reset_index()
df_total_cur_oc = df_total_cur_oc.groupby(['razon_social_oc','anio']).count().reset_index().iloc[:,:3]
df_total_cur_oc = df_total_cur_oc.rename(columns={'nombre_curso_perfil':'total_cursos'})
df_fact_oc = pd.merge(df_fact_oc,df_razon_social_oc,left_on=['razon_social_oc'],right_on=['razon_social'],how="inner")
df_fact_oc = pd.merge(df_fact_oc,df_curso_perfil,left_on=['razon_social_oc','nombre_curso_perfil'],right_on=['razon_social','curso_perfil'],how="inner")
df_fact_oc = pd.merge(df_fact_oc,df_fecha_oc,left_on=['anio'],right_on=['anio'],how="inner")


# In[192]:


#df_fact_oc.head()
#df_fact_oc.info()


# In[195]:


df_fact_oc = pd.merge(df_fact_oc,df_total_cap_oc,on=['razon_social_oc','anio'],how="inner")
df_fact_oc = pd.merge(df_fact_oc,df_total_cur_oc,on=['razon_social_oc','anio'],how="inner")


# In[196]:


df_fact_oc = df_fact_oc[['id_razon_social','id_curso_perfil','id_fecha','num_cap_cer','carga_horaria','total_cursos','volumen_capacitados']]
df_fact_oc['total_horas'] = df_fact_oc['num_cap_cer']*df_fact_oc['carga_horaria']
df_fact_oc.drop('carga_horaria',axis=1,inplace=True)


# In[197]:


df_fact_oc['total_horas'] = df_fact_oc['total_horas'].astype('int64')


# In[224]:


#df_fact_oc.head()
#df_fact_oc.info()


# In[200]:


df_curso_perfil.drop('razon_social',axis=1,inplace=True)


# In[202]:


df_curso_perfil.to_csv(os.path.join(path_dm_oc,'dim_curso_perfil.csv'),index=False)
df_fact_oc.to_csv(os.path.join(path_dm_oc,'fact_oc.csv'),index=False)


# # Dataframe OEC

# ### Dimensión Razon Social OEC

# In[203]:


df_razon_social_oec = df_razon_social[df_razon_social['tipo_razon_social']=='organismo evaluador de la conformidad']


# In[37]:


#df_razon_social_oec.head()
#df_razon_social_oec.info()


# ### Dimension Perfil

# In[207]:


df_perfil = pd.read_csv(os.path.join(path_db,'oec_dl_familia_sector_perfil.csv'))
df_perfil = df_perfil.rename(columns={'Nombre':'razon_social','Familia':'familia','Sector':'sector','Perfil':'perfil'})
df_perfil = df_perfil.rename_axis('id_perfil').reset_index().astype('object')
df_perfil['modalidad'] = 'desconocida'
df_perfil = df_perfil.apply(lambda x: x.astype(str).str.lower())
#df_perfil['carga_horaria'] = df_perfil['carga_horaria'].astype('float64')
df_perfil['carga_horaria'] = 6
df_perfil['costo'] = 250.0


# In[209]:


#df_perfil.head()
#df_perfil.info()


# ## Hechos OEC

# In[225]:


#df_pcoec = pd.read_csv(os.path.join(path_db,'pcoec.csv'))
df_demanda_oec = df_pcoec.groupby(['razon_social','perfil','fecha_certificacion']).count().reset_index().iloc[:,:4]


# In[214]:


#df_demanda_oec.head()
#df_demanda_oec.info()


# ## Dimension Fecha

# In[226]:


df_fecha_oec = df_pcoec[['fecha_certificacion']].groupby(['fecha_certificacion']).count().reset_index()
data_fecha = df_fecha_oec['fecha_certificacion'].str.rsplit("-", expand=True)


# In[227]:


df_fecha_oec = pd.concat([df_fecha_oec,data_fecha],axis=1)
df_fecha_oec = df_fecha_oec.rename(columns={0:'dia',1:'mes',2:'anio'})
df_fecha_oec = df_fecha_oec.rename_axis('id_fecha').reset_index().astype('str')


# In[228]:


#df_fecha_oec.head()
#df_fecha_oec.info()


# In[229]:


df_demanda_oec = df_demanda_oec.rename(columns={'apellidos_nombres':'num_cer'})
df_total_cap_oec = df_pcoec.groupby(['razon_social','fecha_certificacion']).count().reset_index()
df_total_cap_oec = df_total_cap_oec.rename(columns={'numero_certificacion':'volumen_capacitados'})
df_total_cap_oec = df_total_cap_oec[['razon_social','fecha_certificacion','volumen_capacitados']]
df_total_cur_oec = df_pcoec.groupby(['razon_social','perfil','fecha_certificacion']).count().reset_index()
df_total_cur_oec = df_total_cur_oec.groupby(['razon_social','fecha_certificacion']).count().reset_index()
df_total_cur_oec = df_total_cur_oec.rename(columns={'numero_certificacion':'total_cursos'})
df_total_cur_oec = df_total_cur_oec[['razon_social','fecha_certificacion','total_cursos']]
df_demanda_oec = pd.merge(df_demanda_oec,df_total_cap_oec,on=['razon_social','fecha_certificacion'],how="inner")
df_demanda_oec = pd.merge(df_demanda_oec,df_total_cur_oec,on=['razon_social','fecha_certificacion'],how="inner")
df_demanda_oec = pd.merge(df_demanda_oec,df_razon_social_oec,on='razon_social',how="inner")
df_demanda_oec = pd.merge(df_demanda_oec,df_perfil,on=['razon_social','perfil'],how="inner")
df_demanda_oec = pd.merge(df_demanda_oec,df_fecha_oec,on='fecha_certificacion',how="inner")
df_demanda_oec['total_horas'] = df_demanda_oec['num_cer']*df_demanda_oec['carga_horaria']
df_demanda_oec['total_ganacias'] = df_demanda_oec['num_cer']*df_demanda_oec['costo']
df_demanda_oec = df_demanda_oec[['id_razon_social','id_perfil','id_fecha','num_cer','total_horas','total_cursos','volumen_capacitados','total_ganacias']]


# In[233]:


#df_demanda_oec.head()
#df_demanda_oec.info()


# In[236]:


df_fecha_oec = df_fecha_oec.drop('fecha_certificacion',axis=1)
df_fecha_oec.to_csv(os.path.join(path_dm_oec,'dim_fecha.csv'),index=False)


# In[237]:


df_perfil = df_perfil.drop('razon_social',axis=1)
df_perfil.to_csv(os.path.join(path_dm_oec,'dim_perfil.csv'),index=False)


# In[238]:


df_demanda_oec.to_csv(os.path.join(path_dm_oec,'fact_oec.csv'),index=False)


# # Dataframe CI

# ## Dimensión Razon Social CI

# In[242]:


df_razon_social_ci = df_razon_social[df_razon_social['tipo_razon_social']=='capacitador independiente']


# In[243]:


df_razon_social_ci['anio'] = df_razon_social_ci['fecha_resolucion'].apply(get_year)


# In[248]:


#df_razon_social_ci.tail()
#df_razon_social_ci.info()


# ### Dimesión Fecha CI

# In[249]:


df_fecha_ci = pd.DataFrame(data=df_razon_social_ci['anio'].unique(),columns=['anio'])
df_fecha_ci = df_fecha_ci.rename_axis('id_fecha').reset_index().astype('str')


# ## Dimensión Curso

# In[250]:


#df_ci_lca = pd.read_csv(os.path.join(path_db,'ci_lca.csv'))
df_ci_lca =  df_ci_lca.rename_axis('id_curso').reset_index().astype('str')
df_ci_lca["carga_horaria"]=df_ci_lca["carga_horaria"].astype('int64')
df_ci_lca.insert(6,'modalidad', 'desconocida')


# In[271]:


#df_ci_lca.tail()
# df_ci_lca.info()


# ## Hechos CI

# In[255]:





# In[268]:


df_pcci = pd.read_csv(os.path.join(path_db,'pcci.csv'))
df_pcci = df_pcci.rename(columns={'capacitador_independiente':'razon_social'})
df_pcci = pd.merge(df_pcci,df_razon_social_ci[['anio','razon_social']],on=['razon_social'],how="inner")
df_pcci = df_pcci.rename(columns={'numero_documento':'num_cap'})
#df_razon_social_ci = df_razon_social_ci.drop(['anio'],axis=1)
fact_ci = df_pcci.groupby(['razon_social','curso','anio']).count().reset_index()
fact_ci = pd.merge(fact_ci,df_razon_social_ci[['razon_social','id_razon_social']],on=['razon_social'],how="inner")


# In[260]:


#fact_ci.head()
#fact_ci.info()


# In[272]:


df_pcci = pd.read_csv(os.path.join(path_db,'pcci.csv'))
df_pcci = df_pcci.rename(columns={'capacitador_independiente':'razon_social'})
df_pcci = pd.merge(df_pcci,df_razon_social_ci[['anio','razon_social']],on=['razon_social'],how="inner")
df_pcci = df_pcci.rename(columns={'numero_documento':'num_cap'})
df_ci_lca = df_ci_lca.rename(columns={'apellidos_nombres':'razon_social'})
fact_ci = df_pcci.groupby(['razon_social','curso','anio']).count().reset_index()
fact_ci = pd.merge(fact_ci,df_razon_social_ci[['razon_social','id_razon_social']],on=['razon_social'],how="inner")
df_total_cap_ci = df_pcci.groupby(['razon_social','anio']).count().reset_index()
df_total_cap_ci = df_total_cap_ci.rename(columns={'num_cap':'volumen_capacitados'})
df_total_cap_ci = df_total_cap_ci[['razon_social','anio','volumen_capacitados']]
df_total_cur_ci = df_pcci.groupby(['razon_social','curso','anio']).count().reset_index()
df_total_cur_ci = df_total_cur_ci.groupby(['razon_social','anio']).count().reset_index()
df_total_cur_ci = df_total_cur_ci.rename(columns={'num_cap':'total_cursos'})
df_total_cur_ci = df_total_cur_ci[['razon_social','anio','total_cursos']]
fact_ci = pd.merge(fact_ci,df_total_cap_ci,on=['razon_social','anio'],how="inner")
fact_ci = pd.merge(fact_ci,df_total_cur_ci,on=['razon_social','anio'],how="inner")
fact_ci = pd.merge(fact_ci,df_ci_lca,on=['razon_social','curso'],how="inner")
fact_ci = pd.merge(fact_ci,df_fecha_ci,left_on=['anio'],right_on=['anio'],how="inner")
fact_ci['num_cap'] = fact_ci['num_cap'].astype('int64')
fact_ci['total_horas'] = fact_ci['num_cap']*fact_ci['carga_horaria']
fact_ci['total_horas'] = fact_ci['total_horas'].astype('int64')
fact_ci = fact_ci[['id_razon_social','id_curso','id_fecha','num_cap','total_horas','total_cursos','volumen_capacitados','carga_horaria']]


# In[275]:


#fact_ci.tail()
#fact_ci.info()
# fact_ci.describe(include='all')


# ### Guardar Datamart

# In[276]:


df_ci_lca = df_ci_lca[['id_curso', 'curso', 'area','especialidad', 'modalidad', 'carga_horaria']]
df_ci_lca.to_csv(os.path.join(path_dm_ci,'dim_curso_ci.csv'),index=False)


# In[277]:


df_razon_social_ci = df_razon_social_ci.drop(['anio'],axis=1)
df_razon_social_ci.to_csv(os.path.join(path_dm_ci,'dim_razon_social_ci.csv'),index=False)
df_fecha_ci.to_csv(os.path.join(path_dm_ci,'dim_fecha_ci.csv'),index=False)
fact_ci.to_csv(os.path.join(path_dm_ci,'fact_ci.csv'),index=False)


# In[445]:


df_razon_social.to_csv(os.path.join(path_dwh,'dim_razon_social.csv'),index=False)


# # Costos

# In[279]:


#Filtrar las areas de Tecnologia y TICs
df_dim_curso_perfil = pd.read_csv(os.path.join(path_dm_oc,'dim_curso_perfil.csv'))
df_dim_curso_perfil = df_dim_curso_perfil[(df_dim_curso_perfil['area_familia']=='información y comunicación')|(df_dim_curso_perfil['area_familia'] == 'tecnologías de la información y comunicación')]
data_1= df_dim_curso_perfil.groupby(['curso_perfil','modalidad']).count().reset_index()
data_1 = data_1[['curso_perfil','modalidad']]

df_dim_curso = pd.read_csv(os.path.join(path_dm_ci,'dim_curso_ci.csv'))
df_dim_curso = df_dim_curso[df_dim_curso['area']=='tecnologías de la información y comunicación']
data_3 = df_dim_curso.groupby('curso').count().reset_index()
data_3['modalidad'] = 'desconocida'
data_3 = data_3[['curso','modalidad']]

arr_dims_curso_perfil = [df_dim_curso_perfil,df_dim_curso]


# In[280]:


data = np.concatenate((data_1.values,data_3.values),axis=0)
data = pd.DataFrame(data=data,columns=['curso_perfil','modalidad'])
data = data.drop_duplicates()
data['aux_2_curso_perfil'] = data['curso_perfil'].str.cat(data['modalidad'],sep=' | ')
data['aux_curso_perfil'] = data['curso_perfil'].apply(lambda x: normalizar(x))


# In[282]:


df_coec = pd.read_csv(os.path.join(path_db,'costos_estimados_cursos.csv'))
df_coec['aux_2_curso'] = df_coec['curso'].str.cat(df_coec['modalidad'],sep=' | ')
df_coec['aux_curso'] = df_coec['curso'].apply(lambda x: normalizar(x))


# In[285]:


arr = []

for i in range(len(df_coec['aux_curso'])):
    # Similaridad 
    sub_arr = []
    for j in range(len(data['aux_curso_perfil'])):
        curso_fuente = df_coec['aux_curso'].iloc[i]
        curso_comparar = data['aux_curso_perfil'].iloc[j]
        similary = lev.similarity(curso_fuente,curso_comparar,(1,1.5,1.25))
        aux_similary=0
        arr_curso = curso_comparar.split(' ')
        if curso_comparar in curso_fuente :
            similary = 1.0
        else:
            for word in arr_curso:
                if word in curso_fuente:
                    aux_similary+=1/len(arr_curso)
            if data['modalidad'].iloc[j] == df_coec['modalidad'].iloc[i]:
                aux_similary += aux_similary/2
        if aux_similary > similary and aux_similary > 2/len(arr_curso):
            similary = aux_similary
        sub_arr.append(similary)
    arr.append(sub_arr)


# In[286]:


res = pd.DataFrame(data=arr,index=df_coec['aux_2_curso'],columns=data['aux_2_curso_perfil'])


# In[288]:


arr_dims_cols = ['curso_perfil','curso']
arr_datamarts = [path_dm_oc,path_dm_ci]
arr_files = ['dim_curso_perfil.csv','dim_curso_ci.csv']
arr_df_final_dims = []
for i in range(len(arr_dims_curso_perfil)):
  df_dim = arr_dims_curso_perfil[i]
  df_dim['curso_modalidad'] = df_dim[arr_dims_cols[i]].str.cat(df_dim['modalidad'],sep=' | ')
  arr_curso_fuente = []
  for j in range(arr_dims_curso_perfil[i].shape[0]):
      query = df_dim['curso_modalidad'].iloc[j]
      val = res[res[query]==res[query].max()][query].index.values[0]
      arr_curso_fuente.append(df_coec[df_coec['aux_2_curso']==val][['curso','precio_por_hora']].values[0])
  df_curso_fuente_precio = pd.DataFrame(data=arr_curso_fuente,columns=['curso_fuente','precio_por_hora'])
  df_res_corr = pd.DataFrame(data=[df_dim[arr_dims_cols[i]].values,df_dim['modalidad'].values,df_curso_fuente_precio['curso_fuente'].values,df_curso_fuente_precio['precio_por_hora'].values])
  df_res_corr = df_res_corr.transpose()
  df_res_corr = df_res_corr.rename(columns={0:arr_dims_cols[i],1:'modalidad',2:'curso_fuente',3:'precio_por_hora'})
  df_dim = pd.read_csv(os.path.join(arr_datamarts[i],arr_files[i]))
  df_dim= pd.merge(df_dim,df_res_corr,left_on=[arr_dims_cols[i],'modalidad'],right_on=[arr_dims_cols[i],'modalidad'],how='left')
  df_dim['costo'] = df_dim['carga_horaria']*df_dim['precio_por_hora']
  df_dim = df_dim.drop(['curso_fuente','precio_por_hora'], axis=1)
  df_dim = df_dim.drop_duplicates()
  df_dim.to_csv(os.path.join(arr_datamarts[i],arr_files[i]),index=False)
  arr_df_final_dims.append(df_dim)


# ## Agregar Total precio

# In[289]:


df_dim_curso_perfil = pd.read_csv(os.path.join(path_dm_oc,'dim_curso_perfil.csv'))
df_fact_oc = pd.read_csv(os.path.join(path_dm_oc,'fact_oc.csv'))
df_fact_oc_mod = pd.merge(df_fact_oc,df_dim_curso_perfil,on='id_curso_perfil',how='inner')
df_fact_oc_mod['total_ganancias'] = df_fact_oc_mod['num_cap_cer']*df_fact_oc_mod['costo']
df_fact_oc_mod.fillna({'total_ganancias':0},inplace=True)
df_fact_oc_mod_res = df_fact_oc_mod[['id_razon_social','id_curso_perfil','id_fecha','num_cap_cer','total_cursos','volumen_capacitados','total_horas','total_ganancias']]


# In[290]:


df_dim_curso = pd.read_csv(os.path.join(path_dm_ci,'dim_curso_ci.csv'))
df_fact_ci = pd.read_csv(os.path.join(path_dm_ci,'fact_ci.csv'))
df_fact_ci_mod = pd.merge(df_fact_ci,df_dim_curso,on='id_curso',how='inner')
df_fact_ci_mod['total_ganancias'] = df_fact_ci_mod['num_cap']*df_fact_ci_mod['costo']
df_fact_ci_mod.fillna({'total_ganancias':0},inplace=True)
df_fact_ci_mod_res = df_fact_ci_mod[['id_razon_social','id_curso','id_fecha','num_cap','total_cursos','volumen_capacitados','total_horas','total_ganancias']]


# In[293]:


#df_fact_ci_mod_res.info()


# In[292]:


df_fact_oc_mod_res.to_csv(os.path.join(path_dm_oc,'fact_oc.csv'),index=False)
df_fact_ci_mod_res.to_csv(os.path.join(path_dm_ci,'fact_ci.csv'),index=False)


# In[305]:


df_razon_social = df_razon_social.drop('fecha_resolucion',axis=1)


# In[354]:


df_razon_social.columns


# ## % de Asistencia

# ### OC

# In[443]:


df_fact_oc = pd.read_csv(os.path.join(path_dm_oc,'fact_oc.csv'))
df_fact_oc['id_razon_social'] = df_fact_oc['id_razon_social'].astype('str')
df_oc_num_conv= pd.read_csv(os.path.join(os.getcwd(),'pcoc_num_conv.csv'))[['razon_social', 'numero_convocados']]
df_fact_oc = pd.merge(df_fact_oc,df_razon_social[['id_razon_social','razon_social']],on='id_razon_social',how='left')
df_fact_oc = pd.merge(df_fact_oc,df_oc_num_conv,on='razon_social',how='left')
df_fact_oc['%_asistencia'] = (df_fact_oc['num_cap_cer']/df_fact_oc['numero_convocados'])*100
df_fact_oc = df_fact_oc.drop(['razon_social', 'numero_convocados'],axis=1)
df_fact_oc.fillna({'%_asistencia':0},inplace=True)
df_fact_oc.to_csv(os.path.join(path_dm_oc,'fact_oc.csv'),index=False)


# ### OEC

# In[444]:


df_fact_oec = pd.read_csv(os.path.join(path_dm_oec,'fact_oec.csv'))
df_fact_oec['id_razon_social'] = df_fact_oec['id_razon_social'].astype('str')
df_oec_num_conv= pd.read_csv(os.path.join(os.getcwd(),'pcoec_num_conv.csv'),encoding = "ISO-8859-1")[['razon_social', 'numero_convocados']]
df_fact_oec = pd.merge(df_fact_oec,df_razon_social[['id_razon_social','razon_social']],on='id_razon_social',how='left')
df_fact_oec = pd.merge(df_fact_oec,df_oec_num_conv,on='razon_social',how='left')
df_fact_oec['%_asistencia'] = (df_fact_oec['num_cer']/df_fact_oec['numero_convocados'])*100
df_fact_oec = df_fact_oec.drop(['razon_social', 'numero_convocados'],axis=1)
df_fact_oec.fillna({'%_asistencia':0},inplace=True)
df_fact_oec.to_csv(os.path.join(path_dm_oec,'fact_oec.csv'),index=False)


# # CARGA DE DATOS

# # Conexión con MySQL

# In[426]:


mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    passwd='KappaTao1233'
)

mycursor = mydb.cursor()


# # Creación de la base de datos

# In[427]:


mycursor.execute("DROP DATABASE IF EXISTS dwh_setec")
mycursor.execute("CREATE DATABASE dwh_setec")


# In[428]:


mycursor.execute("USE dwh_setec")


# # DIMENSIÓN RAZÓN SOCIAL

# In[429]:


#os.listdir(path_dwh)


# In[430]:


df_razon_social.info()


# In[431]:


mycursor.execute("DROP TABLE IF EXISTS dim_razon_social")
mycursor.execute('''CREATE TABLE dim_razon_social (
id_razon_social VARCHAR(8) PRIMARY KEY,
razon_social VARCHAR(255) NOT NULL,
tipo_razon_social VARCHAR(255) NOT NULL,
estado VARCHAR(63) NOT NULL,
canton VARCHAR(63) NOT NULL,
provincia VARCHAR(63) NOT NULL
)''')
sql_formula = 'INSERT INTO dim_razon_social ( id_razon_social, razon_social,tipo_razon_social, estado, canton, provincia) VALUES (%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_razon_social.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# # DATAMART OC

# In[432]:


#os.listdir(path_dm_oc)


# ## Tabla dim_curso_perfil_oc

# In[433]:


df_cur_per = pd.read_csv(os.path.join(path_dm_oc,'dim_curso_perfil.csv'))
df_cur_per['id_curso_perfil'] = df_cur_per['id_curso_perfil'].astype(str)
df_cur_per = df_cur_per.fillna({'costo':0})


# In[434]:


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

# In[435]:


df_fecha_oc = pd.read_csv(os.path.join(path_dm_oc,'dim_fecha.csv'))
df_fecha_oc['id_fecha'] = df_fecha_oc['id_fecha'].astype(str)
df_fecha_oc['anio'] = df_fecha_oc['anio'].astype(str)


# In[436]:


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

# In[437]:


df_fact_oc = pd.read_csv(os.path.join(path_dm_oc,'fact_oc.csv'))
df_fact_oc['id_razon_social'] = df_fact_oc['id_razon_social'].astype(str)
df_fact_oc['id_curso_perfil'] = df_fact_oc['id_curso_perfil'].astype(str)
df_fact_oc['id_fecha'] = df_fact_oc['id_fecha'].astype(str)


# In[438]:


#df_fact_oc.info()


# In[439]:


mycursor.execute("DROP TABLE IF EXISTS fact_oc")
mycursor.execute('''CREATE TABLE fact_oc (
id_razon_social VARCHAR(4) NOT NULL,
id_curso_perfil VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cap_cer INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_horas INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = '''INSERT INTO fact_oc (id_razon_social, id_curso_perfil, id_fecha, num_cap_cer, total_cursos, volumen_capacitados, total_horas, total_ganancias) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''
for row in tqdm(df_fact_oc.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## CREAR CLAVES FORANEAS OC

# In[440]:


mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_oc(id_fecha)
''')
mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_razon_social) REFERENCES dim_razon_social(id_razon_social)
''')
mycursor.execute(''' 
ALTER TABLE fact_oc
ADD FOREIGN KEY (id_curso_perfil) REFERENCES dim_curso_perfil_oc(id_curso_perfil)
''')


# # DATAMART OEC

# In[441]:


#os.listdir(path_dm_oec)


# In[442]:


df_oec = pd.read_csv(os.path.join(path_dmoec,'dim_razon_social_oec.csv'))
df_oec['id_oec'] = df_oec['id_oec'].astype(str)


# In[ ]:


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

# In[ ]:


df_perfil = pd.read_csv(os.path.join(path_dm_oec,'dim_perfil.csv'))
df_perfil['id_perfil'] = df_perfil['id_perfil'].astype(str)


# In[ ]:


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

# In[ ]:


df_fecha_oec = pd.read_csv(os.path.join(path_dm_oec,'dim_fecha.csv'))
df_fecha_oec['id_fecha'] = df_fecha_oec['id_fecha'].astype(str)


# In[ ]:


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

# In[ ]:


df_fact_oec = pd.read_csv(os.path.join(path_dm_oec,'fact_oec.csv'))
df_fact_oec['id_razon_social'] = df_fact_oec['id_razon_social'].astype(str)
df_fact_oec['id_perfil'] = df_fact_oec['id_perfil'].astype(str)
df_fact_oec['id_fecha'] = df_fact_oec['id_fecha'].astype(str)


# In[ ]:


mycursor.execute("DROP TABLE IF EXISTS fact_oec")
mycursor.execute('''CREATE TABLE fact_oec (
id_razon_social VARCHAR(4) NOT NULL,
id_perfil VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cer INT NOT NULL,
total_horas INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO fact_oec(id_razon_social, id_perfil, id_fecha, num_cer, total_horas, total_cursos, volumen_capacitados, total_ganancias) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_fact_oec.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# ## CREAR CLAVES FORANEAS OEC

# In[ ]:


mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_oec(id_fecha)
''')
mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_razon_social) REFERENCES dim_razon_social(id_razon_social)
''')
mycursor.execute(''' 
ALTER TABLE fact_oec
ADD FOREIGN KEY (id_perfil) REFERENCES dim_perfil_oec(id_perfil)
''')


# # DATAMART CI

# In[ ]:


#os.listdir(path_dm_ci)


# ## Tabla dim_curso

# In[ ]:


df_curso = pd.read_csv(os.path.join(path_dm_ci,'dim_curso_ci.csv'))
df_curso['id_curso'] = df_curso['id_curso'].astype(str)
df_curso = df_curso.fillna({'costo':0})
df_curso = df_curso.dropna()


# In[ ]:


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

# In[ ]:


df_fecha_ci = pd.read_csv(os.path.join(path_dm_ci,'dim_fecha_ci.csv'))
df_fecha_ci['id_fecha'] = df_fecha_ci['id_fecha'].astype(str)


# In[ ]:


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

# In[ ]:


df_fact_ci = pd.read_csv(os.path.join(path_dm_ci,'fact_ci.csv'))
df_fact_ci['id_razon_social'] = df_fact_ci['id_razon_social'].astype(str)
df_fact_ci['id_curso'] = df_fact_ci['id_curso'].astype(str)
df_fact_ci['id_fecha'] = df_fact_ci['id_fecha'].astype(str)


# In[ ]:


mycursor.execute("DROP TABLE IF EXISTS fact_ci")
mycursor.execute('''CREATE TABLE fact_ci (
id_razon_social VARCHAR(4) NOT NULL,
id_curso VARCHAR(4) NOT NULL,
id_fecha VARCHAR(4) NOT NULL,
num_cap INT NOT NULL,
total_cursos INT NOT NULL,
volumen_capacitados INT NOT NULL,
total_horas INT NOT NULL,
total_ganancias FLOAT NOT NULL
)''')
sql_formula = 'INSERT INTO fact_ci(id_razon_social, id_curso, id_fecha, num_cap, total_cursos, volumen_capacitados, total_horas, total_ganancias) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
for row in tqdm(df_fact_ci.values):
    mycursor.execute(sql_formula,tuple(row))
    mydb.commit()


# In[ ]:


#df_ci.info()
# ', '.join(df_fact_ci.columns)


# ## CREAR CLAVES FORANEAS CI

# In[ ]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_fecha) REFERENCES dim_fecha_ci(id_fecha)
''')


# In[ ]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_razon_social) REFERENCES dim_razon_social(id_razon_social)
''')


# In[ ]:


mycursor.execute(''' 
ALTER TABLE fact_ci
ADD FOREIGN KEY (id_curso) REFERENCES dim_curso_ci(id_curso)
''')

