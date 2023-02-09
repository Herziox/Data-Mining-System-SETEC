#!/usr/bin/env python
# coding: utf-8

# # Librerías 

# In[2]:


#Libraries
from csv import writer
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from time import time
import numpy as np
from datetime import datetime
from threading import Thread, Barrier
import copy
import sys
import os


# # Variables de los distitos xpaths de la pagina


# In[4]:


xpath_URL = ''
xpath_MODULE = ''
xpath_FILTER = ''
xpath_OPTION = ''
xpath_TEXTBOX_QUERY = ''
xpath_BOTTON_SEARCH = ''
xpath_BOTTON_LAST_PAGE = ''
xpath_BOTTON_NEXT_PAGE = ''
xpath_JUMP_PAGE = ''
xpath_TABLE = ''
BUTTON_DETAIL = -1
DETAIL = ''
xpath_TABLE_DETAIL = ''
xpath_BOTTON_DETAIL_NEXT_PAGE = ''
xpath_BOTTON_EXIT = ''
columns = []
FOLDER = ''
SUBFOLDER = ''
xpath_EXPIRED_SESSION = '/html/body/div[2]/div/span[3]'
xpath_FAIL_SESSION ='/html/body/div[2]/div/span[1]'
regist_columns = ['time_bot', 'id_bot', 'doc_name', 'session', 'failed_session','expired_session', 'start_page', 'end_page', 'page', 'n_rows', 'n_cols', 'message']
cache_path = ''
report_path = ''
data_path = ''

# # Funciones de guardado

# In[5]:

# Funcion encargada de llevar los registros de del proceso de web scrapping del bot
def save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, message):
    regist = [str(datetime.now()), id_bot, doc_name,sessions,failed_sessions,expired_sessions,start_page, end_page, page, n_rows, n_cols, message]  
    
    if os.path.exists(report_path):
      report = pd.read_csv(report_path)
      if regist[-1] == report.iloc[-1]['message']:
        report.iloc[-1] = regist
        report.to_csv(report_path,index=False)
      else:
        report = pd.DataFrame(data=[regist],columns=regist_columns)
        report.to_csv(report_path,index=False, mode='a', header=not os.path.exists(report_path))
    else:
      report = pd.DataFrame(data=[regist],columns=regist_columns)
      report.to_csv(report_path,index=False, mode='a', header=not os.path.exists(report_path))


#Funcion encargada de guardar los datos extraidos en el CSV dentro de la carpeta, subcarpeta y archivo correcto
def save_info(id_bot,doc_name,data,sessions,failed_sessions,expired_sessions,start_page,end_page,page,message):
    df = pd.DataFrame(data=data,columns=columns)
    df.to_csv(data_path,index=False)
    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page,df.shape[0], df.shape[1], message)


# # Función número de páginas

# In[6]:


def get_final_page(options,QUERY_INPUT=' ',try_load=True):
#TRY TO LOAD PAGE
  while(try_load):
    driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

    ## GET TABLE
    try:

      ## URL
      print('Get URL')
      driver.get(xpath_URL)

      ## MODULE
      print('Get Module')
      driver.implicitly_wait(5)
      btn_module = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.LINK_TEXT, xpath_MODULE)))
      driver.execute_script('arguments[0].click()',btn_module)

      ## SELECT FILTER
      print('Select Filter')
      driver.implicitly_wait(5)
      cmb_seleccione_filtro = driver.find_element(By.ID, xpath_FILTER)
      cmb_seleccione_filtro.click()

      ## SELECT OPTION
      print('Select Option')
      driver.implicitly_wait(5)
      cmb_item_nombre_curso = driver.find_element(By.ID, xpath_OPTION)
      cmb_item_nombre_curso.click()

      ## WRITE QUERY
      print('Write Query')
      driver.implicitly_wait(5)
      sleep(2)
      txt_nombre_curso_perfil = driver.find_element(By.ID, xpath_TEXTBOX_QUERY)
      txt_nombre_curso_perfil.click()
      driver.implicitly_wait(5)
      txt_nombre_curso_perfil.send_keys(QUERY_INPUT)

      ## SEARCH QUERY
      print('Search Query')
      driver.implicitly_wait(5)
      btn_buscar = driver.find_element(By.ID, xpath_BOTTON_SEARCH)
      btn_buscar.click()           
      sleep(40)
      
      #GET LAST PAGE
      print('Get Last Page')
      btn_control = driver.find_element(By.XPATH, xpath_BOTTON_LAST_PAGE)
      driver.execute_script('arguments[0].click()',btn_control)
      sleep(5)
      page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
      end_page = int(page_active[1].text)
      return end_page
      # CONFIRM SUCCESSFUL LOAD
      try_load = False
    except Exception as e:
      print('ERROR GET FINAL PAGE: ',e)
      try_load = True
      driver.quit()
  driver.quit() 
  return end_page


# # Función Bot

# In[7]:


def bot(id_bot,doc_name,options,start_page,end_page,QUERY_INPUT=' '):
  
  # Iniciar Web Scraper
  go = True

  # Numero de sesiones
  sessions=0

  # Numero de sesiones fallidas
  failed_sessions=0

  # Numero de sesiones expiradas
  expired_sessions=0

  # Pagina a minar
  page = start_page 

  # Numero de filas del documento
  n_rows= 0

  # Numero de columnas del documento
  n_cols=0

  #  Mensaje de log
  message='START PROGRAM'

  # Reporte de minado
  report = ''
  
  
  


  # PRICIPAL LOOP
  while(go):
    
    #Verificar si existe REPORTE de ws
    try:
      report = pd.read_csv(report_path)

      sessions = report['session'].iloc[-1]

      failed_sessions = report['failed_session'].iloc[-1]

      expired_sessions = report['expired_session'].iloc[-1]

      page = report['page'].iloc[-1] 

      n_rows= report['n_rows'].iloc[-1]

      n_cols= report['n_cols'].iloc[-1]

      message='RESTART PROGRAM'

    except Exception as e:
      print('REPORT FILE ERROR: ',e)
    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, message)

    try_load = True

    #TRY TO LOAD PAGE
    while(try_load):
      driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

      ## GET TABLE
      try:
        sessions += 1

        ## URL
        driver.get(xpath_URL)
        
        ## MODULE
        driver.implicitly_wait(5)
        btn_module = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.LINK_TEXT, xpath_MODULE)))
        driver.execute_script('arguments[0].click()',btn_module)

        ## SELECT FILTER
        driver.implicitly_wait(5)
        cmb_seleccione_filtro = driver.find_element(By.ID, xpath_FILTER)
        cmb_seleccione_filtro.click()

        ## SELECT OPTION
        driver.implicitly_wait(5)
        cmb_item_nombre_curso = driver.find_element(By.ID, xpath_OPTION)
        cmb_item_nombre_curso.click()

        ## WRITE QUERY
        driver.implicitly_wait(5)
        txt_nombre_curso_perfil = driver.find_element(By.ID, xpath_TEXTBOX_QUERY)
        txt_nombre_curso_perfil.click()
        driver.implicitly_wait(5)
        txt_nombre_curso_perfil.send_keys(QUERY_INPUT)

        ## SEARCH QUERY
        driver.implicitly_wait(5)
        btn_buscar = driver.find_element(By.ID, xpath_BOTTON_SEARCH)
        btn_buscar.click()           

        # CONFIRM SUCCESSFUL LOAD
        try_load = False
      except Exception as e:
        print(e)
        failed_sessions += 1
        try_load = True
        driver.quit() 

    #START - WRITE REPORT
    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, 'Portal Web Loaded, Go to Page')      
    #END - WRITE REPORT

    expired_session = False
    naeip = 0
    detail_id = 0
    page_number=0
    page_checkpoint = page


    # FIND last page
    while(page_number<page_checkpoint and page_number<end_page and page_checkpoint<end_page and not expired_session):
        try:
            btn_siguiente = driver.find_element(By.XPATH, xpath_BOTTON_NEXT_PAGE)

            if (page_checkpoint - page_number)>10:
              btn_siguiente = driver.find_element(By.XPATH, xpath_JUMP_PAGE)

            detail_id +=20
            page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
            page_number = int(page_active[1].text) + 1
            btn_siguiente.click()



            if (page_number>=end_page):
                #START - WRITE REPORT
                save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_checkpoint, n_rows, n_cols, 'FINAL MINED')      
                #END - WRITE REPORT
                break

        except:
          if (naeip>5):
            #START - WRITE REPORT
            save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_checkpoint, n_rows, n_cols, 'Check Expired session in FIND LAST PAGE MINED')      
            #END - WRITE REPORT
            try:
              #Check Expired session
                span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                if 'Sesión Caducada' == span_expired_session.text:
                  expired_sessions +=1
                  expired_session = True
                  #START - WRITE REPORT
                  save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_checkpoint, n_rows, n_cols, 'Expired session')      
                  #END - WRITE REPORT
                  break
                span_failed_session = driver.find_element(By.XPATH, xpath_FAIL_SESSION)
                if 'Ocurrio una Excepcion Grave' == span_failed_session.text:
                  failed_sessions +=1
                  expired_session = True
                  #START - WRITE REPORT
                  save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_checkpoint, n_rows, n_cols, 'falied session')      
                  #END - WRITE REPORT
                  break
                print('NAEIP exceded in FIND LAST PAGE MINED')
                try_load = False
            except Exception as e :
                print("Error Checking Page: ",e)
                naeip = 0
          else:
            naeip += 1

    page = page_number
    if (page_number >= end_page):
      go = False
      break

    elif(not expired_session):

      #START - WRITE REPORT
      save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page,page_number, n_rows, n_cols, 'Resume Mining')      
      #END - WRITE REPORT


      #TRY TO READ CSV
      data = np.zeros((0,len(columns)))
      sleep(1)
      try:
        df_data = pd.read_csv(data_path)
        data = df_data.values
      except:
        #START - WRITE REPORT
        save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'CSV Not Found')      
        #END - WRITE REPORT

      condition = True

      while(condition):

        try_load = True

        #Number of Attempts to Extract Information from the Page
        naeip = 0

        while(try_load):
          try:
            table_data=''
            if xpath_TABLE == 'tbody':
              table_data = driver.find_element(By.TAG_NAME,xpath_TABLE)
            else: 
              table_data = driver.find_element(By.ID,xpath_TABLE)
            row_data = table_data.find_elements(By.TAG_NAME,'tr')
            table_page =[]
            if DETAIL != 'detalle':
                for row in row_data:
                  cell = row.find_elements(By.TAG_NAME,'td')
                  reg = [val.text for val in cell]
                  table_page.append(reg[:len(columns)])
                  detail_id+=1
                data=np.append(data,np.array(table_page),axis=0)
            else:
                for row in row_data:
                  cell = row.find_elements(By.TAG_NAME,'td')
                  reg = [val.text for val in cell]
                  detail_button = cell[BUTTON_DETAIL].find_element(By.CLASS_NAME,'ui-button-icon-only')
                  driver.execute_script('arguments[0].click()',detail_button)              
                  condition_cc = False
                  ## GET TABLE HEADER

                  try:
                    driver.find_element(By.ID,xpath_TABLE_DETAIL)
                    condition_cc = True
                  except:
                    condition_cc = False
                    #Pass

                  # Capacitación Continua
                  while(condition_cc):

                    try_load_cc = True

                    # naeip = Number of Attempts to Extract Information from the Page
                    naeip_cc = 0

                    while(try_load_cc):
                      try:      
                        table_cc = driver.find_element(By.ID,xpath_TABLE_DETAIL)
                        row_detail = table_cc.find_elements(By.TAG_NAME,'tr')
                        table_data_cc = []
                        for row_d in row_detail:
                          cell_d = row_d.find_elements(By.TAG_NAME,'td')
                          reg_d = [val_d.text for val_d in cell_d]
                          reg_aux = reg[:2]+reg_d
                          table_data_cc.append(reg_aux)
                        data = np.append(data,np.array(table_data_cc),axis=0)
                        try_load_cc = False

                      except Exception: 
                        e = sys.exc_info()[1]
                        print("Error data CC: ",e.args[0])
                        if 'no such element' in str(e.args[0]):
                          print('Datos de CC no encontrados')
                          condition_cc = False
                          try_load_cc = False
                          break
                        if naeip_cc>=5:
                          try:
                              save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Check Expired session in TABLE SCRAP')
                            #Check Expired session
                              span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                              if 'Sesión Caducada' == span_expired_session.text:
                                expired_sessions +=1
                                expired_session = True
                                #START - WRITE REPORT
                                save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Expired session')      
                                #END - WRITE REPORT
                                break
                              span_failed_session = driver.find_element(By.XPATH, xpath_FAIL_SESSION) 
                              if 'Ocurrio una Excepcion Grave' == span_failed_session.text:
                                failed_sessions +=1
                                expired_session = True
                                #START - WRITE REPORT
                                save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'falied session')      
                                #END - WRITE REPORT
                                break
                              print('NAEIP exceded in CC')
                              try_load = False
                          except Exception as e :
                              print("Error Checking: ",e)
                              naeip_cc = 0
                        else:
                          #print('carga CC ',naeip_cc)
                          naeip_cc += 1
                          try_load_cc = True #FIN



                    try:
                      driver.implicitly_wait(30)
                      #button_next_page_cc = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="frmAreaEspecialidadOc:dataTable_paginator_bottom"]/a[3]')))
                      button_next_page_cc =  driver.find_element(By.XPATH,xpath_BOTTON_DETAIL_NEXT_PAGE)

                      # SAVE INFORMATION ABOUT MINING CC
                      save_info(id_bot,doc_name,data,sessions,failed_sessions,expired_sessions,start_page,end_page,page_number,'Saved Data')
                      button_next_page_cc.click()
                    except Exception: 
                      e = sys.exc_info()[1]
                      print("Error data CLICK CC: ",e.args[0])
                      condition_cc = False
                      button_exit = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,xpath_BOTTON_EXIT)))
                      button_exit.click()
                      break
            


            try_load = False

          except Exception:
            e = sys.exc_info()[1]
            print("Error data: ",e.args[0])
            if naeip>=5:
              save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Check Expired session in FIND LAST PAGE DETAIL MINED')
              try:
                #Check Expired session
                 
                  span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                  if 'Sesión Caducada' == span_expired_session.text:
                    expired_sessions +=1
                    expired_session = True
                    #START - WRITE REPORT
                    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Expired session')      
                    #END - WRITE REPORT
                    break
                  span_failed_session = driver.find_element(By.XPATH, xpath_FAIL_SESSION) 
                  if 'Ocurrio una Excepcion Grave' == span_failed_session.text:
                    failed_sessions +=1
                    expired_session = True
                    #START - WRITE REPORT
                    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'falied session')      
                    #END - WRITE REPORT
                    break
                  print('NAEIP exceded')
                  try_load = False
                  break
              except Exception as e :
                  print("Error Checking: ",e)
                  naeip = 0
            else:
              naeip += 1
              try_load = True            


        try:
          # SAVE INFORMATION ABOUT MINING
          page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
          page_number = int(page_active[1].text)
          page = page_number
          driver.implicitly_wait(30)
          tiempo_data_fin = time()
          save_info(id_bot,doc_name,data,sessions,failed_sessions,expired_sessions,start_page,end_page,page_number,'Saved Data')

          if (page_number>=end_page):
            go = False
            break

          #NEXT PAGE
          driver.implicitly_wait(30)
          btn_siguiente = driver.find_element(By.XPATH, xpath_BOTTON_NEXT_PAGE)
          btn_siguiente.click()

        except:
          condition = False
          #START - WRITE REPORT
          save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Fail Click')      
          #END - WRITE REPORT
          break

        #print('Numero de filas: ',len(data), '\n')


      #START - WRITE REPORT
      save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'End Session')      

      if(page_number<end_page):
        go = True
      else:
        go = False
        break
      driver.quit()
    driver.quit()
  


# # Función Principal

# In[ ]:

def main_bot(thread,i,doc_name,options,start_page,end_page,QUERY_INPUT):
    bot(i,doc_name,options,start_page,end_page,QUERY_INPUT)
    thread.wait()



if __name__==('__main__'):
  
  doc_name = sys.argv[1]
  QUERY_INPUT = ' '
  N_DRIVERS = int(sys.argv[3])
  new_ws = sys.argv[4]
  id_bot = int(sys.argv[5])
  df_control = pd.read_csv(os.path.join(os.getcwd(),'data_controller_bot.csv'))
  df_control = df_control[df_control['doc_name'] == doc_name]
  if df_control.shape[0]>0:
    if '_' == sys.argv[2]:
      QUERY_INPUT = ' '
    elif '-' == sys.argv[2]:
      QUERY_INPUT = ''
    else:
      QUERY_INPUT = sys.argv[2]
    
    # Asignar los xpaths a las variables 
    columns = df_control['columns'].iloc[0].replace('\"','').replace('\'','').replace('[','').replace(']','').split(',')
    xpath_URL = df_control['xpath_URL'].iloc[0]
    xpath_MODULE = df_control['xpath_MODULE'].iloc[0]
    xpath_FILTER = df_control['xpath_FILTER'].iloc[0]
    xpath_OPTION = df_control['xpath_OPTION'].iloc[0]
    xpath_TEXTBOX_QUERY = df_control['xpath_TEXTBOX_QUERY'].iloc[0]
    xpath_BOTTON_SEARCH = df_control['xpath_BOTTON_SEARCH'].iloc[0]
    xpath_BOTTON_LAST_PAGE = df_control['xpath_BOTTON_LAST_PAGE'].iloc[0]
    xpath_BOTTON_NEXT_PAGE = df_control['xpath_BOTTON_NEXT_PAGE'].iloc[0]
    xpath_JUMP_PAGE = df_control['xpath_JUMP_PAGE'].iloc[0]
    xpath_TABLE = df_control['xpath_TABLE'].iloc[0]
    BUTTON_DETAIL = df_control['BUTTON_DETAIL'].iloc[0]
    DETAIL = df_control['DETAIL'].iloc[0]
    xpath_TABLE_DETAIL = df_control['xpath_TABLE_DETAIL'].iloc[0]
    xpath_BOTTON_DETAIL_NEXT_PAGE = df_control['xpath_BOTTON_DETAIL_NEXT_PAGE'].iloc[0]
    xpath_BOTTON_EXIT = df_control['xpath_BOTTON_EXIT'].iloc[0]
    FOLDER = df_control['folder'].iloc[0]
    SUBFOLDER = df_control['subfolder'].iloc[0]
  else:
    print('No existe este modulo')
  
  cache_path = os.path.join(FOLDER,SUBFOLDER,"cache_bots_"+doc_name+".txt")
  report_path = os.path.join(FOLDER,SUBFOLDER,'report_file_'+doc_name+'_'+str(id_bot)+'.csv')
  data_path = os.path.join(FOLDER,SUBFOLDER, doc_name+'_'+str(id_bot)+'.csv') 

  
  # Opciones del scraper para ejecutar el broser
  options = webdriver.ChromeOptions() 
  # to supress the error messages/logs

  options.add_experimental_option('excludeSwitches', ['enable-logging'])
  
  # Estas opciones permiten ejecutar sin abrir un browser
  #options.add_argument('--headless')
  #options.add_argument('--no-sandbox')
  #options.add_argument('--disable-dev-shm-usage')

  final_page=1
  start_page_list = []
  final_page_list = []
  
  # Archivo Cache que recolecta la informacion del numero de paginas a extraer y las subdivide
  # en el numero de scrapers bots que queremos que realizar la extraccion al mismo tiempo

  if new_ws == 'yes' and os.path.exists(cache_path):
    os.remove(cache_path)      
        
  if os.path.exists(os.path.join(cache_path)):
    print('Cache File Found')
    cache_file = open(cache_path, "r")
    value_bot = cache_file.readline().split(' ')
    final_page = int(value_bot[0])
    N_DRIVERS = int(value_bot[1])
    cache_file.close()  
  else:
    print('New File')
    final_page = get_final_page(options,QUERY_INPUT)
    cache_file = open(cache_path, "w")
    cache_file.write(str(final_page)+' '+str(N_DRIVERS))
    cache_file.close()  
  
  print(final_page) 
    
    
  
  range_page = final_page//N_DRIVERS
  mark_page = 0
  for i in range(N_DRIVERS):
    start_page_list.append(mark_page)
    mark_page+=range_page
    if(mark_page>final_page):
      final_page_list.append(final_page)
    else:
      final_page_list.append(mark_page)
  
  # LLamada al bot
  print('Inicia el Bot ',doc_name,'_',id_bot)
  bot(id_bot,doc_name,options,start_page_list[id_bot],final_page_list[id_bot],QUERY_INPUT)

  


