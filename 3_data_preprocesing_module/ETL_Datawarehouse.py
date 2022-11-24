#!/usr/bin/env python
# coding: utf-8

# # Integración de Datos Datawarehouse

# ## Importación de librerías

# In[1]:


import pandas as pd
import numpy as np
import os
import re


# # Ambiente de Google Colab de ser necesario

# In[ ]:


#from google.colab import drive
#drive.mount('/content/drive')


# # Ruta de Archivos

# In[5]:


base_path = os.getcwd()
path = os.path.join(base_path, "CLEANED_DATA")


# # Datawarehouse

# In[6]:


#path_datawarehouse = '/content/drive/MyDrive/Trabajo_de_Integracion_Curricular/CODE/DataWareHouse/'
path_datawarehouse = os.path.join(base_path, "DATAWAREHOUSE")
path_datamart_oc = 'datamart_oc'
path_datamart_oec = 'datamart_oec'
path_datamart_ci = 'datamart_ci'


#  ## Tabla Ubicacion

# In[44]:


df_ubicacion = pd.read_csv(os.path.join(path_datawarehouse, 'ubicacion.csv'))


# # Datamart OC

# ## Dimensión Razon Social OC

# In[45]:


df_oc = pd.read_csv(os.path.join(path,'oc.csv'))
df_oc_ug = pd.read_csv(os.path.join(path,'oc_dl_provincia_canton.csv'))
df_oc_ug['Estado']='DESCONOCIDO'
df_oc_ug['Razon_Social']=df_oc_ug['Nombre']
df_razon_social_oc = pd.concat([df_oc[['Razon_Social','Estado','Canton']],df_oc_ug[['Razon_Social','Estado','Canton']]],axis=0)
df_razon_social_oc = df_razon_social_oc.drop_duplicates(subset=['Razon_Social','Canton'],keep='first')
df_razon_social_oc=df_razon_social_oc.rename(columns={'Razon_Social':'nombre','Canton':'canton','Estado':'estado'})
df_razon_social_oc = pd.merge(df_razon_social_oc,df_ubicacion,on='canton',how="left")
df_razon_social_oc.pop('id_ubicacion')
df_razon_social_oc = df_razon_social_oc.fillna({'provincia':'Sin Provincia','Provincia':'Sin Provincia'})
df_razon_social_oc = df_razon_social_oc.rename_axis('id_oc').reset_index().astype('object')
#df_razon_social_oc.info()


# In[ ]:


df_razon_social_oc.to_csv(os.path.join(path_datawarehouse,path_datamart_oc,'dim_razon_social_oc.csv'),index=False)


# ## Dimensión Curso_Perfil

# In[37]:


df_oc_cl = pd.read_csv(os.path.join(path,'oc_dl_familia_sector_perfil.csv'))
df_oc_cl_2 = pd.read_csv(os.path.join(path,'oc_cl.csv'))
df_oc_cl_2.pop('RUC_o_Codigo')
df_oc_cl_2 = df_oc_cl_2.rename(columns={'Razon_Social':'Nombre'})
df_oc_cl = pd.concat([df_oc_cl,df_oc_cl_2],axis=0)
df_oc_cl = df_oc_cl.drop_duplicates()
df_oc_cl['modalidad'] = np.NaN
df_oc_cl['carga_horaria'] = np.NaN
df_oc_cl['tipo'] = 'perfil'
df_oc_cc = pd.read_csv(os.path.join(path,'oc_cc.csv'))
df_oc_cc.pop('documento')
df_oc_cc['tipo'] = 'curso'


# In[38]:


data_curso_perfil = np.concatenate((df_oc_cc.values,df_oc_cl.values), axis=0)
df_curso_perfil = pd.DataFrame(data=data_curso_perfil,columns=['razon_social','area_familia','especialidad_sector','curso_perfil','modalidad','carga_horaria','tipo'])
df_curso_perfil.insert(0,'tipo',df_curso_perfil.pop('tipo'))
df_curso_perfil = df_curso_perfil.rename_axis('id_curso_perfil').reset_index().astype('object')
#df_curso_perfil.info()


# In[39]:


df_curso_perfil.to_csv(os.path.join(path_datawarehouse,path_datamart_oc,'dim_curso_perfil.csv'),index=False)


# ## Hechos OC

# In[40]:


#AGREGAR AÑO
# Cargar los archivos necesarios de la ubicación de CLEANED DATA
df_pcoc = pd.read_csv(os.path.join(path,'pcoc.csv'))

# Se extraera el año de la columna Numero_certificado.
# Para esto se buscará porciones de texto que contengan los años del 2000 - 2022 (año actual)
year_list = [str(x) for x in range(2022,2009,-1)]

def get_year(x):
    for year in year_list:
        if re.search('-'+year+'\s|'+year+'-|-'+year+'$',x):
            return year
    return 'sin_anio'

df_pcoc['anio'] = df_pcoc['Numero_Certificado'].apply(get_year)

df_oc = pd.read_csv(os.path.join(path,'oc.csv'))

df_extract_0_oc = df_oc[['Razon_Social','Fecha_Resolucion']]

df_pcoc = pd.merge(df_pcoc,df_extract_0_oc,left_on=['Razon_Social_OC'],right_on=['Razon_Social'],how="left") 

def get_year_by_oc(x,y):
    if x=='sin_anio':
        if y != 'Sin Fecha Resolución' and not pd.isna(y):
            return y[-4:]
    else:
        return x

df_pcoc['anio'] = df_pcoc.apply(lambda x: get_year_by_oc(x['anio'], x['Fecha_Resolucion']), axis=1)

df_pcoc[
    (df_pcoc['anio']=='sin_anio') & 
    (df_pcoc['Fecha_Resolucion']!='Sin Fecha Resolución') &
    (df_pcoc['Fecha_Resolucion'].notnull())
    ]['anio'] = df_pcoc[
    (df_pcoc['anio']=='sin_anio') & 
    (df_pcoc['Fecha_Resolucion']!='Sin Fecha Resolución') &
    (df_pcoc['Fecha_Resolucion'].notnull())
    ]['Fecha_Resolucion'].str.slice(start=-4)

df_pcoc = df_pcoc.drop(['Razon_Social','Fecha_Resolucion'],axis=1)


# ## Dimension Fecha OC
# 
# Se debe desarrollar en base a el año el cual se haya obtenido el certificado del curso o perfil OC.
# 
# En caso de no existir un año registrado en el certificado en la tabla **PCOC** se deberá tomar como referencia el año de resolución de la Razon social de la tabla **OC**

# In[41]:



df_fecha = pd.DataFrame(data=df_pcoc['anio'].unique(),columns=['anio'])
df_fecha = df_fecha.rename_axis('id_fecha').reset_index().astype('object')
df_fecha.to_csv(os.path.join(path_datawarehouse,path_datamart_oc,'dim_fecha.csv'),index=False)


# In[49]:


## UNIR DATOS
df_fact_oc = df_pcoc.groupby(['Razon_Social_OC','Nombre_Curso_Perfil','anio']).count().reset_index().iloc[:,:4]
df_fact_oc = df_fact_oc.rename(columns={'Numero_Documento':'num_cap_cer'})
df_fact_oc = pd.merge(df_fact_oc,df_razon_social_oc,left_on=['Razon_Social_OC'],right_on=['nombre'],how="left")
df_fact_oc = pd.merge(df_fact_oc,df_curso_perfil,left_on=['Razon_Social_OC','Nombre_Curso_Perfil'],right_on=['razon_social','curso_perfil'],how="left")
df_fact_oc = pd.merge(df_fact_oc,df_fecha,left_on=['anio'],right_on=['anio'],how="left")
df_fact_oc = df_fact_oc[['id_oc','id_curso_perfil','id_fecha','num_cap_cer']]


# In[122]:


df_curso_perfil.pop('razon_social')
df_curso_perfil.to_csv(os.path.join(path_datawarehouse,path_datamart_oc,'dim_curso_perfil.csv'),index=False)


# In[111]:


df_fact_oc.to_csv(os.path.join(path_datawarehouse,path_datamart_oc,'fact_oc.csv'),index=False)


# # Dataframe OEC

# In[ ]:



# ### Dimensión Razon Social OEC

# In[136]:


df_oec_2 = pd.read_csv(os.path.join(path,'oec_dl_provincia_canton.csv'))
df_oec_2['estado'] = 'DESCONOCIDO'
df_oec_2 = df_oec_2.rename(columns={'Nombre':'razon_social','Cantón':'canton','Provincia':'provincia'})
df_oec_2 = df_oec_2[['razon_social','estado','canton']]
df_oec = pd.read_csv(os.path.join(path,'oec.csv'))
df_razon_social_oec = df_oec[['Razon_Social','Estado','Canton']]
df_razon_social_oec=df_razon_social_oec.rename(columns={'Razon_Social':'razon_social','Canton':'canton','Estado':'estado'})
df_razon_social_oec = pd.concat([df_razon_social_oec,df_oec_2])
df_razon_social_oec = pd.merge(df_razon_social_oec,df_ubicacion,on='canton',how="left")
df_razon_social_oec.pop('id_ubicacion')
df_razon_social_oec = df_razon_social_oec.fillna({'provincia':'Sin Provincia'})
df_razon_social_oec = df_razon_social_oec.drop_duplicates(subset=['razon_social'], keep='first')
df_razon_social_oec = df_razon_social_oec.rename_axis('id_oec').reset_index().astype('object')
#df_razon_social_oec.info()


# In[130]:


df_razon_social_oec.to_csv(os.path.join(path_datawarehouse,path_datamart_oec,'dim_razon_social_oec.csv'),index=False)


# ### Dimension Perfil

# In[134]:


df_perfil = pd.read_csv(os.path.join(path,'oec_dl_familia_sector_perfil.csv'))
df_perfil = df_perfil.rename(columns={'Nombre':'razon_social','Familia':'familia','Sector':'sector','Perfil':'perfil'})
df_perfil = df_perfil.rename_axis('id_perfil').reset_index().astype('object')
#df_perfil.info()


# In[137]:


df_perfil.to_csv(os.path.join(path_datawarehouse,path_datamart_oec,'dim_perfil.csv'),index=False)


# ## Hechos OEC

# In[155]:


df_pcoec = pd.read_csv(os.path.join(path,'pcoec.csv'))
df_demanda_oec = df_pcoec.groupby(['OEC','Perfil','Fecha_Certificacion']).count().reset_index().iloc[:,:4]
df_demanda_oec = df_demanda_oec.rename(columns={'Nombres':'num_cer'})
df_demanda_oec = pd.merge(df_demanda_oec,df_razon_social_oec,left_on=['OEC'],right_on=['razon_social'],how="left")


# ## Dimension Fecha

# In[150]:


df_fecha = df_demanda_oec['Fecha_Certificacion'].drop_duplicates()
df_fecha = df_fecha.rename_axis('id_fecha').reset_index().astype('object')
data_fecha = df_fecha['Fecha_Certificacion'].str.rsplit("-", expand=True)
df_fecha = pd.concat([df_fecha,data_fecha],axis=1)
df_fecha = df_fecha.rename(columns={0:'dia',1:'mes',2:'año'})
#df_fecha.info()


# In[151]:


df_demanda_oec = df_pcoec.groupby(['OEC','Perfil','Fecha_Certificacion']).count().reset_index().iloc[:,:4]
df_demanda_oec = df_demanda_oec.rename(columns={'Nombres':'num_cer'})
df_demanda_oec = pd.merge(df_demanda_oec,df_razon_social_oec,left_on=['OEC'],right_on=['razon_social'],how="left")
df_demanda_oec = pd.merge(df_demanda_oec,df_perfil,left_on=['OEC','Perfil'],right_on=['razon_social','perfil'],how="left")
df_demanda_oec = pd.merge(df_demanda_oec,df_fecha,left_on=['Fecha_Certificacion'],right_on=['Fecha_Certificacion'],how="left")
df_demanda_oec = df_demanda_oec[['id_oec','id_perfil','id_fecha','num_cer']]
#df_demanda_oec.info()


# In[152]:


df_fecha = df_fecha.drop('Fecha_Certificacion',axis=1)
df_fecha.to_csv(os.path.join(path_datawarehouse,path_datamart_oec,'dim_fecha.csv'),index=False)


# In[154]:


df_demanda_oec.to_csv(os.path.join(path_datawarehouse,path_datamart_oec,'fact_oec.csv'),index=False)


# # Dataframe CI

# In[45]:




# ## Dimensión Capacitador independiente

# In[103]:


df_capacitador_ci = pd.read_csv(os.path.join(path,'ci.csv'))
df_capacitador_ci = df_capacitador_ci.rename(columns={'Apellidos_Nombres':'razon_social','Codigo_Resolucion':'codigo_resolucion'})
df_capacitador_ci = df_capacitador_ci[['razon_social','codigo_resolucion']]
df_capacitador_ci_2 = pd.read_csv(os.path.join(path,'ci_dl_provincia_canton.csv'))
df_capacitador_ci_2 = df_capacitador_ci_2.rename(columns={'Nombre':'razon_social'})
df_capacitador_ci = pd.merge(df_capacitador_ci,df_capacitador_ci_2,left_on=['razon_social'],right_on=['razon_social'],how="left")
df_capacitador_ci_2.insert(1,'codigo_resolucion','DESCONOCIDO')
df_capacitador_ci = pd.concat([df_capacitador_ci,df_capacitador_ci_2],axis=0)
df_capacitador_ci = df_capacitador_ci.drop_duplicates(subset=['razon_social','Provincia','Cantón'],keep='first')
df_capacitador_ci = df_capacitador_ci.rename(columns={'Provincia':'provincia','Cantón':'canton'})
df_capacitador_ci = df_capacitador_ci.fillna({'provincia':'Sin Provincia','canton':'Sin Canton'})
df_capacitador_ci['anio'] = df_capacitador_ci['codigo_resolucion'].apply(get_year)
df_capacitador_ci  = df_capacitador_ci.rename_axis('id_ci').reset_index().astype('object')
#df_capacitador_ci.info()


# In[109]:


df_fecha = pd.DataFrame(data=df_capacitador_ci['anio'].unique(),columns=['anio'])
df_fecha = df_fecha.rename_axis('id_fecha').reset_index().astype('object')
df_fecha


# ## Dimensión Curso

# In[130]:


df_ci_lca = pd.read_csv(os.path.join(path,'ci_lca.csv'))
df_ci_lca = df_ci_lca.rename(columns={'Nombre_Curso':'curso','Area':'area','Especialidad':'especialidad','Carga Horaria':'carga_horaria'})
df_ci_lca =  df_ci_lca.rename_axis('id_curso').reset_index().astype('object')
#df_ci_lca.info()


# ## Hechos CI

# In[131]:


df_pcci = pd.read_csv(os.path.join(path,'pcci.csv'))
df_pcci = pd.merge(df_pcci,df_capacitador_ci,left_on=['Instructor_Capacitador'],right_on=['razon_social'],how="left")
df_pcci = df_pcci.groupby(['Instructor_Capacitador','Nombre_Curso','anio']).count().reset_index()[['Instructor_Capacitador','Nombre_Curso','anio','Numero_Documento']]
df_pcci = df_pcci.rename(columns={'Numero_Documento':'num_cap'})

df_pcci = pd.merge(df_pcci,df_ci_lca,left_on=['Instructor_Capacitador','Nombre_Curso'],right_on=['Apellidos_Nombres','curso'],how="left")
df_pcci = pd.merge(df_pcci,df_fecha,left_on=['anio'],right_on=['anio'],how="left")
df_pcci = pd.merge(df_pcci,df_capacitador_ci,left_on=['Instructor_Capacitador'],right_on=['razon_social'],how="left")
fact_ci = df_pcci[['id_ci','id_curso','id_fecha','num_cap']]
#fact_ci.info()


# In[132]:


df_ci_lca = df_ci_lca.drop(['Numero_Documento', 'Apellidos_Nombres'],axis=1)
df_ci_lca.to_csv(os.path.join(path_datawarehouse,path_datamart_ci,'dim_curso.csv'),index=False)
df_capacitador_ci.to_csv(os.path.join(path_datawarehouse,path_datamart_ci,'dim_capacitador_independiente.csv'),index=False)
df_fecha.to_csv(os.path.join(path_datawarehouse,path_datamart_ci,'dim_fecha.csv'),index=False)
fact_ci.to_csv(os.path.join(path_datawarehouse,path_datamart_ci,'fact_ci.csv'),index=False)

