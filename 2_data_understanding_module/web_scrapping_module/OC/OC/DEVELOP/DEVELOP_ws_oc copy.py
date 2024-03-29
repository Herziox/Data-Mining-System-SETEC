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
import threading
import copy
import sys
import os


# # Variables

# In[3]:


# xpath_URL = 'http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf'
# xpath_FILTER = 'j_idt24:pnlOrganismo:cmbSubOc:cmbSubOc_label'
# xpath_OPTION = 'j_idt24:pnlOrganismo:cmbSubOc:cmbSubOc_3'
# xpath_TEXTBOX_QUERY = 'j_idt24:pnlOrganismo:txtRazonSocial:txtRazonSocial'
# xpath_BOTTON_SEARCH = 'j_idt24:pnlOrganismo:j_idt43'
# xpath_BOTTON_LAST_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/a[4]'
# xpath_BOTTON_NEXT_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/a[3]'
# xpath_JUMP_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/span/a[10]'
# xpath_TABLE = 'j_idt24:pnlOrganismo:tblDatosTabla_data' 
# xpath_EXPIRED_SESSION = '/html/body/div[2]/div/span[3]'
# regist_columns = ['time_bot', 'id_bot', 'doc_name', 'session', 'failed_session','expired_session', 'start_page', 'end_page', 'page', 'n_rows', 'n_cols', 'message']
# columns = ['ruc_o_codigo','razon_social', 'nombre_comercial', 'telefono',	'celular',	'correo_electronico',	'numero_resolucion', 'fecha_resolucion',	'estado', 'canton']


# In[4]:


xpath_URL = 'http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacionIndependientes.jsf'
xpath_FILTER = 'j_idt24:frmCapInd:cmbSubOcIndependiente:cmbSubOcIndependiente_label'
xpath_OPTION = 'j_idt24:frmCapInd:cmbSubOcIndependiente:cmbSubOcIndependiente_5'
xpath_TEXTBOX_QUERY = 'j_idt24:frmCapInd:txtParametroCurso:txtParametroCurso'
xpath_BOTTON_SEARCH = 'j_idt24:frmCapInd:j_idt58'
xpath_BOTTON_LAST_PAGE = '//*[@id="j_idt24:frmCapInd:tbl4_paginator_bottom"]/a[4]'
xpath_BOTTON_NEXT_PAGE = '//*[@id="j_idt24:frmCapInd:tbl4_paginator_bottom"]/a[3]'
xpath_JUMP_PAGE = '//*[@id="j_idt24:frmCapInd:tbl4_paginator_bottom"]/span/a[10]'
xpath_TABLE = 'j_idt24:frmCapInd:tbl4_data'
BUTTON_DETAIL = -2
DETAIL = 'detalle'
xpath_TABLE_DETAIL = 'frmAreaEspecialidadOc:dataTable_data'
xpath_BOTTON_DETAIL_NEXT_PAGE = '//*[@id="frmAreaEspecialidadOc:dataTable_paginator_bottom"]/a[3]'
xpath_BOTTON_EXIT = '//*[@id="frmAreaEspecialidadOc:j_idt124"]/div[1]/a'
xpath_EXPIRED_SESSION = '/html/body/div[2]/div/span[3]'
regist_columns = ['time_bot', 'id_bot', 'doc_name', 'session', 'failed_session','expired_session', 'start_page', 'end_page', 'page', 'n_rows', 'n_cols', 'message']
columns = ['ruc_o_codigo','razon_social','codigo_resolucion','vigencia_resolucion','correo_electronico','celular']
detail_columns = ['ruc_o_codigo','razon_social','curso','area','especialidad','carga_horaria']

# # Funciones de guardado

# In[5]:


def save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, message):
    regist = [[str(datetime.now()), id_bot, doc_name,sessions,failed_sessions,expired_sessions,start_page, end_page, page, n_rows, n_cols, message]]
    report = pd.DataFrame(data=regist,columns=regist_columns)   
    report_path = 'report_file_'+doc_name+'_'+str(id_bot)+'.csv'
    report.to_csv(report_path,index=False, mode='a', header=not os.path.exists(report_path))

def save_info(id_bot,doc_name,data,sessions,failed_sessions,expired_sessions,start_page,end_page,page,message):
    df = pd.DataFrame(data=data,columns=columns)
    data_path = doc_name+'_'+str(id_bot)+'.csv'
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
      driver.get(xpath_URL)

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
      #GET LAST PAGE
      btn_control = driver.find_element(By.XPATH, xpath_BOTTON_LAST_PAGE)
      driver.execute_script('arguments[0].click()',btn_control)
      sleep(3)
      page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
      end_page = int(page_active[1].text)

      # CONFIRM SUCCESSFUL LOAD
      try_load = False
    except:
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

  #Verificar si existe REPORTE de ws
  try:
        report = pd.read_csv(report_path)

        sessions = report['sessions'].iloc[-1]

        failed_sessions = report['failed_sessions'].iloc[-1]

        expired_sessions = report['expired_sessions'].iloc[-1]

        page = report['page'].iloc[-1] 

        n_rows= report['n_rows'].iloc[-1]

        n_cols= report['n_cols'].iloc[-1]

        message='RESTART PROGRAM'

  except:
        pass
  save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, message)


  # PRICIPAL LOOP
  while(go):  

    try_load = True

    #TRY TO LOAD PAGE
    while(try_load):
      driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

      ## GET TABLE
      try:
        sessions += 1

        ## URL
        driver.get(xpath_URL)

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
    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page, n_rows, n_cols, 'Page Loaded, Go to Page')      
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
                save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'FINAL MINED')      
                #END - WRITE REPORT
                break

        except:
          if (naeip>10):
            #START - WRITE REPORT
            save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Check Expired session')      
            #END - WRITE REPORT
            try:
              #Check Expired session
                span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                if 'Sesión Caducada' == span_expired_session.text:
                  n_expired_sessions +=1
                  expired_session = True
                  #START - WRITE REPORT
                  save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Expired session')      
                  #END - WRITE REPORT
                  break
            except:
                naeip = 0
          else:
            naeip += 1


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
        df_data = pd.read_csv(doc_name+'_'+str(id_bot)+'.csv')
        data = df_data.values
      except:
        #START - WRITE REPORT
        save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'CSV Not Found')      
        #END - WRITE REPORT

      data_cc = np.zeros((0,len(detail_columns)))
      try:
        df_cc = pd.read_csv(doc_name+'_cc_'+str(id_bot)+'.csv')
        data_cc = df_cc.values
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
            table_data = driver.find_element(By.ID,xpath_TABLE)
            row_data = table_data.find_elements(By.TAG_NAME,'tr')
            table_page =[]
            if DETAIL == '':
                for row in row_data:
                  cell = row.find_elements(By.TAG_NAME,'td')
                  reg = [val.text for val in cell]         
                  table_page.append(reg[:-2])
                  detail_id+=1
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
                        print(table_data_cc)
                        data_cc = np.append(data_cc,np.array(table_data_cc),axis=0)
                        try_load_cc = False


                      except Exception: 
                        e = sys.exc_info()[1]
                        print("Error data CC: ",e.args[0])
                        if 'no such element' in str(e.args[0]):
                          print('Datos de CC no encontrados')
                          condition_cc = False
                          try_load_cc = False
                          break
                        if naeip_cc>=10:
                          try:
                            #Check Expired session
                              span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                              if 'Sesión Caducada' == span_expired_session.text:
                                n_expired_sessions +=1
                                expired_session = True
                                #START - WRITE REPORT
                                save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Expired session')      
                                #END - WRITE REPORT
                                break
                          except:
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
                      df_cc = pd.DataFrame(data=data_cc,columns=detail_columns)
                      df_cc.to_csv(doc_name+'_cc'+str(id_bot)+'.csv',index=False)

                      button_next_page_cc.click()
                    except Exception: 
                      e = sys.exc_info()[1]
                      print("Error data CLICK CC: ",e.args[0])
                      condition_cc = False
                      button_exit = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="frmAreaEspecialidadOc:j_idt124"]/div[1]/a')))
                      button_exit.click()
                      break
            data=np.append(data,np.array(table_page),axis=0)


            try_load = False

          except Exception:
            e = sys.exc_info()[1]
            print("Error data: ",e.args[0])
            if naeip>=10:
              try:
                #Check Expired session
                  span_expired_session = driver.find_element(By.XPATH, xpath_EXPIRED_SESSION)
                  if 'Sesión Caducada' == span_expired_session.text:
                    n_expired_sessions +=1
                    expired_session = True
                    #START - WRITE REPORT
                    save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, end_page, page_number, n_rows, n_cols, 'Expired session')      
                    #END - WRITE REPORT
                    break
              except:
                  naeip = 0
            else:
              naeip += 1
              try_load = True            


        try:
          # SAVE INFORMATION ABOUT MINING
          page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
          page_number = int(page_active[1].text)
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
      #END - WRITE REPORT

      save_info(id_bot,doc_name,data,sessions,failed_sessions,expired_sessions,start_page,end_page,page_number,'Saved Data')

      if(page_number<end_page):
        go = True
      else:
        go = False
        #START - WRITE REPORT
        save_regist(id_bot, doc_name, sessions,failed_sessions,expired_sessions, start_page, page_number, page, n_rows, n_cols, 'END PROGRAM')      
        #END - WRITE REPORT
        break
      driver.quit()
    driver.quit()
  


# # Función Principal

# In[ ]:


if __name__==('__main__'):
  N_DRIVERS = 1
  QUERY_INPUT = ' '
  doc_name = 'ci'

  options = webdriver.ChromeOptions() 
  # to supress the error messages/logs

  options.add_experimental_option('excludeSwitches', ['enable-logging'])
  #options.add_argument('--headless')
  #options.add_argument('--no-sandbox')
  #options.add_argument('--disable-dev-shm-usage')

  
'''
  final_page = get_final_page(options,QUERY_INPUT)
  print(final_page)
  start_page_list = []
  final_page_list = []

  range_page = final_page//N_DRIVERS
  mark_page = 0
  for i in range(N_DRIVERS):
    start_page_list.append(mark_page)
    mark_page+=range_page
    if(mark_page>final_page):
      final_page_list.append(final_page)
    else:
      final_page_list.append(mark_page)

  print(start_page_list)
  print(final_page_list)
  '''



  #webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))
  #,start_page_list[i],final_page_list[i]

bot(0,doc_name,options,0,5,QUERY_INPUT)
'''
  for i in range(N_DRIVERS):
    browserThread = threading.Thread(target=bot,args=(i,doc_name,options,0,30))
    browserThread.start()
    #bot_poc(i,options,1,final_page,QUERY_INPUT)
'''

