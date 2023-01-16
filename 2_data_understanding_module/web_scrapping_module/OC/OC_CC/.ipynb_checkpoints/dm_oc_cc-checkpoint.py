#Libraries
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

xpath_URL = 'http://portal.trabajo.gob.ec/setec-portal-web/pages/operadoresCapacitacion.jsf'
xpath_FILTER = 'j_idt24:pnlOrganismo:cmbSubOc:cmbSubOc_label'
xpath_OPTION = 'j_idt24:pnlOrganismo:cmbSubOc:cmbSubOc_3'
xpath_TEXTBOX_QUERY = 'j_idt24:pnlOrganismo:txtRazonSocial:txtRazonSocial'
xpath_BOTTON_SEARCH = 'j_idt24:pnlOrganismo:j_idt43'
xpath_BOTTON_LAST_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/a[4]'
xpath_BOTTON_NEXT_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/a[3]'
xpath_JUMP_PAGE = '//*[@id="j_idt24:pnlOrganismo:tblDatosTabla_paginator_bottom"]/span/a[10]'
xpath_TABLE = 'j_idt24:pnlOrganismo:tblDatosTabla_data' 
xpath_EXPIRED_SESSION = '/html/body/div[2]/div/span[3]'
xpath_TABLE_CC = 'frmAreaEspecialidadOc:dataTable_data'
columns = ['RUC_o_Codigo','Razon_Social',	'Nombre_Comercial',	'Telefono',	'Celular',	'Correo_Electronico',	'Numero_Resolucion',	'Fecha_Resolucion',	'Estado', 'Canton']


def save_info(id_bot,doc_name,data,columns,tds,tde,page,start_page,final_page):

    df = pd.DataFrame(data=data,columns=columns)
    df.to_csv(doc_name+str(id_bot)+'.csv',index=False)

    file = open('cache_file'+str(id_bot)+'.txt', "w")
    file.write('Shape_data: '+str(df.shape)+'\n')
    file.write('Page: '+str(page+1)+'\n')
    file.write('Star_Page: '+str(start_page)+'\n')
    file.write('End_Page: '+str(final_page)+'\n')
    time_total = tde-tds
    hours, rem = divmod(time_total,3600)
    minutes, seconds = divmod(rem, 60)
    file.write('Last session time: '+"{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)+'\n')
    file.close()







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
      final_page = int(page_active[1].text)
      
      # CONFIRM SUCCESSFUL LOAD
      try_load = False
    except:
      try_load = True
      driver.quit()
  driver.quit() 
  return final_page




def bot(id_bot,doc_name,options,start_page,final_page,QUERY_INPUT=' '):

  go = True

  n_session = 0

  n_session_falied = 0

  n_expired_sessions = 0

  #Develop
  report_file = open("report_file_"+str(id_bot)+".txt", "a")
  report_file.write('####################--------START PROGRAM---------####################  \n')
  report_file.write('####################  '+ str(datetime.now()) +'  #################### \n')
  report_file.write('===================================================================== \n')
  report_file.write('===================================================================== \n')
  report_file.close()

  # PRICIPAL LOOP
  while(go):  

    try_load = True

    #TRY TO LOAD PAGE
    while(try_load):
      driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

      ## GET TABLE
      try:
        n_session += 1

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
      except:
        n_session_falied += 1
        try_load = True
        driver.quit() 

      #START - WRITE REPORT
      
      report_file = open("report_file_"+str(id_bot)+".txt", "r")
      report_lines = report_file.readlines()
      report_file.close()

      report_lines[-1] = f'Sessions: {n_session} | Falied sessions : {n_session_falied}| Expired sessions: {n_expired_sessions}\n'

      report_file = open("report_file_"+str(id_bot)+".txt", "w")
      report_file.writelines(report_lines)
      report_file.close()
      
      #END - WRITE REPORT


    expired_session = False
    page_checkpoint = start_page
    naeip = 0
    detail_id = 0
    page_number=0


    try:
      #Read the last page mined
      cache_file = open('cache_file'+str(id_bot)+'.txt')
      page_checkpoint = int(cache_file.readlines()[1].split(' ')[1])
      cache_file.close()
    except:
      cache_file = open('cache_file'+str(id_bot)+'.txt', "w")
      cache_lines = f'Shape_data: (0, 0) \nPage: 0 \nStart_Page: 0 \nEnd_Page: 0'
      cache_file.writelines(cache_lines)
      cache_file.close()
      

    #WRITE REPORT
    report_file = open("report_file_"+str(id_bot)+".txt", "a")
    report_file.write('===================================================================== \n')
    report_file.write(f'[Session  {n_session}] | Go to page {page_checkpoint}: {datetime.now()} \n')
    report_file.close()
    
    
    

    # FIND last page
    while(page_number<page_checkpoint and page_number<final_page and page_checkpoint<final_page and not expired_session):
        try:
            btn_siguiente = driver.find_element(By.XPATH, xpath_BOTTON_NEXT_PAGE)

            if (page_checkpoint - page_number)>10:
              btn_siguiente = driver.find_element(By.XPATH, xpath_JUMP_PAGE)
            
            detail_id +=20
            page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
            page_number = int(page_active[1].text) + 1
            btn_siguiente.click()

            

            if (page_number>=final_page):
                #WRITE REPORT
                report_file = open("report_file_"+str(id_bot)+".txt", "a")
                report_file.write('===================================================================== \n')
                report_file.write(f'[Session  {n_session}] | FINAL PAGE {page_number}: {datetime.now()} \n')
                report_file.close()
                break
            
        except:
          if (naeip>10):
            #WRITE REPORT
            report_file = open("report_file_"+str(id_bot)+".txt", "a")
            report_file.write(f'[Session  {n_session}] | Check Expired session: {datetime.now()} \n')
            report_file.close()
            try:
              #Check Expired session
                span_expired_session = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
                if 'Sesión Caducada' == span_expired_session.text:
                  n_expired_sessions +=1
                  expired_session = True
                  #WRITE REPORT
                  report_file = open("report_file_"+str(id_bot)+".txt", "a")
                  report_file.write(f'[Session  {n_session}] | Expired: {datetime.now()} \n')
                  report_file.close()
                  break
            except:
                naeip = 0
          else:
            naeip += 1
          
    
    if (page_number >= final_page):
      go = False
      break
    
    elif(not expired_session):

      #WRITE REPORT
      report_file = open("report_file_"+str(id_bot)+".txt", "a")
      report_file.write(f'[Session  {n_session}] | Resume Mining since page {page_number} : {datetime.now()} \n')
      report_file.close()

      #TRY TO READ CSV
      data = np.zeros((0,len(columns)))
      sleep(1)
      try:
        df_data = pd.read_csv(doc_name+str(id_bot)+'.csv')
        data = df_data.values
      except:
        report_file = open("report_file_"+str(id_bot)+".txt", "a")
        report_file.write(f'[Session  {n_session}] | CSV not found: {datetime.now()} \n')
        report_file.close()
        
      
      data_cc = np.zeros((0,7))
      try:
        df_cc = pd.read_csv(doc_name+'_cc'+str(id_bot)+'.csv')
        data_cc = df_cc.values
      except:
        report_file = open("report_file_"+str(id_bot)+".txt", "a")
        report_file.write(f'[Session  {n_session}] | CSV not found: {datetime.now()} \n')
        report_file.close()
      

      #TIME CONTROL
      tiempo_data_inicio = time()

      condition = True

      while(condition):

        try_load = True

        # naeip = Number of Attempts to Extract Information from the Page
        naeip = 0

        while(try_load):
          try:
            table_data = driver.find_element(By.ID,xpath_TABLE)
            row_data = table_data.find_elements(By.TAG_NAME,'tr')
            table_page =[]
            for row in row_data:
              cell = row.find_elements(By.TAG_NAME,'td')
              reg = [val.text for val in cell]
              cc_button = cell[-1].find_element(By.CLASS_NAME,'ui-button-icon-only')
              #detail_button.click()
              driver.execute_script('arguments[0].click()',cc_button)

########################################### DATOS DE CURSOS DE CAPACITACION #########################################              
              condition_cc = False
              ## GET TABLE HEADER
              
              try:
                driver.find_element(By.ID,xpath_TABLE_CC)
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
                    table_cc = driver.find_element(By.ID,xpath_TABLE_CC)
                    row_detail = table_cc.find_elements(By.TAG_NAME,'tr')
                    table_data_cc = []
                    for row_d in row_detail:
                      cell_d = row_d.find_elements(By.TAG_NAME,'td')
                      reg_d = [val_d.text for val_d in cell_d]
                      reg_aux = reg[:2]+reg_d
                      table_data_cc.append(reg_aux)
                    
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
                          span_expired_session = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
                          if 'Sesión Caducada' == span_expired_session.text:
                            n_expired_sessions +=1
                            expired_session = True
                            report_file = open("report_file_"+str(id_bot)+".txt", "a")
                            report_file.write(f'[Session  {n_session}] | Expired: {datetime.now()} \n')
                            report_file.close()
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
                  button_next_page_cc =  driver.find_element(By.XPATH,'//*[@id="frmAreaEspecialidadOc:dataTable_paginator_bottom"]/a[3]')
                    
                  # SAVE INFORMATION ABOUT MINING CC
                  df_cc = pd.DataFrame(data=data_cc,columns=['documento','razon_social','area','especialidad','curso','modalidad','carga_horaria'])
                  df_cc.to_csv(doc_name+'_cc'+str(id_bot)+'.csv',index=False)
                  
                  button_next_page_cc.click()
                except Exception: 
                  e = sys.exc_info()[1]
                  print("Error data CLICK CC: ",e.args[0])
                  condition_cc = False
                  break
                print('Next row')
                  
            
            try_load = False
          
          except Exception:
            #print(f'Try to read CSV check at {datetime.now()}') 
            e = sys.exc_info()[1]
            print("Error data: ",e.args[0])
            if naeip>=10:
              try:
                #Check Expired session
                  span_expired_session = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
                  if 'Sesión Caducada' == span_expired_session.text:
                    n_expired_sessions +=1
                    expired_session = True
                    report_file = open("report_file_"+str(id_bot)+".txt", "a")
                    report_file.write(f'[Session  {n_session}] | Expired: {datetime.now()} \n')
                    report_file.close()
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
          save_info(id_bot,doc_name,data,columns,tiempo_data_inicio,tiempo_data_fin,page_number,start_page,final_page)
          
          if (page_number>=final_page):
            go = False
            break
          
          #NEXT PAGE
          driver.implicitly_wait(30)
          btn_siguiente = driver.find_element(By.XPATH, xpath_BOTTON_NEXT_PAGE)
          btn_siguiente.click()

        except:
          condition = False
          #WRITE REPORT
          report_file = open("report_file_"+str(id_bot)+".txt", "a")
          report_file.write('========================= Fail click ===========================\n')
          report_file.close()
          break

        #print('Numero de filas: ',len(data), '\n')
        

      tiempo_data_fin = time()

      report_file = open("report_file_"+str(id_bot)+".txt", "a")
      report_file.write(f'[Session  {n_session}] | End Session: {datetime.now()} \n')
      report_file.close()

      #save_info(id_bot,doc_name,data,columns,tiempo_data_inicio,tiempo_data_fin,page_number,start_page,final_page)

      if(page_number<final_page):
        go = True
      else:
        go = False
        break
      driver.quit()
    driver.quit()
  report_file = open("report_file_"+str(id_bot)+".txt", "a")
  report_file.write('############################ END PROGRAM ############################\n')
  report_file.close()



if __name__==('__main__'):

  N_DRIVERS = 1
  QUERY_INPUT = ''
  doc_name = 'oc'

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

bot(0,doc_name,options,0,28,QUERY_INPUT)
'''
  for i in range(N_DRIVERS):
    browserThread = threading.Thread(target=bot,args=(i,doc_name,options,0,30))
    browserThread.start()
    #bot_poc(i,options,1,final_page,QUERY_INPUT)
'''