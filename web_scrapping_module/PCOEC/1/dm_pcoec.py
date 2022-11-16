#Libraries
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
from time import time
import numpy as np
from datetime import datetime
import threading
import copy
import sys



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


def search_query_on_page(driver,QUERY_INPUT):
  ## URL
  driver.get('http://portal.trabajo.gob.ec/setec-portal-web/pages/legitimidadCertificacion.jsf')

  ## SELECT MODULE
  driver.implicitly_wait(5)
  cmb_module = driver.find_element(By.LINK_TEXT, 'Búsqueda Personas Certificadas')
  driver.execute_script('arguments[0].click()',cmb_module)

  ## SELECT FILTER
  driver.implicitly_wait(5)
  cmb_seleccione_filtro = driver.find_element(By.ID, 'j_idt24:frmPersonasCert:cmbCriterioPersona:cmbCriterioPersona_label')
  cmb_seleccione_filtro.click()

  ## SELECT OPTION
  driver.implicitly_wait(5)
  cmb_item_nombre_curso = driver.find_element(By.ID, 'j_idt24:frmPersonasCert:cmbCriterioPersona:cmbCriterioPersona_1')
  cmb_item_nombre_curso.click()
  sleep(2)
  ## WRITE QUERY
  driver.implicitly_wait(5)
  txt_nombre_curso_perfil = driver.find_element(By.ID, 'j_idt24:frmPersonasCert:txtDescripcionPersona:txtDescripcionPersona')
  txt_nombre_curso_perfil.click()
  driver.implicitly_wait(5)
  txt_nombre_curso_perfil.send_keys(QUERY_INPUT)
  

  ## SEARCH QUERY
  driver.implicitly_wait(5)
  btn_buscar = driver.find_element(By.ID, 'j_idt24:frmPersonasCert:j_idt37')
  btn_buscar.click()

  return driver






def get_final_page(options,QUERY_INPUT=' ',try_load=True):
#TRY TO LOAD PAGE
  while(try_load):
    driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

    ## GET TABLE
    try:

      driver = search_query_on_page(driver,QUERY_INPUT)

      #GET LAST PAGE
      btn_control = driver.find_element(By.XPATH,'//*[@id="j_idt24:frmPersonasCert:tblDatosPersonas_paginator_bottom"]/a[4]')
      driver.execute_script('arguments[0].click()',btn_control)
      sleep(3)
      page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
      final_page = int(page_active[1].text)
      
      # CONFIRM SUCCESSFUL LOAD
      try_load = False
    except Exception:
      e = sys.exc_info()[1]
      print("Error data: ",e.args[0])
      try_load = True
      driver.quit()
  driver.quit() 
  return final_page




def bot(id_bot,doc_name,options,start_page,final_page,QUERY_INPUT=' '):
  go = True

  n_session = 0

  n_session_falied = 0

  n_expired_sessions = 0

  data = []

  columns = []

  #Develop
  report_file = open("report_file_"+str(id_bot)+".txt", "a")
  report_file.write('####################--------START PROGRAM---------####################  \n')
  report_file.write('####################  '+ str(datetime.now()) +'  #################### \n')
  report_file.write('===================================================================== \n')
  report_file.write('===================================================================== \n')
  report_file.close()

  while(go):  

    

    time_start_page = time()

    try_load = True

    #TRY TO LOAD PAGE
    while(try_load):
      driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))
      
      ## GET TABLE
      try:
        n_session += 1

        driver = search_query_on_page(driver,QUERY_INPUT)

        #TRY TO READ CSV
        try:
          df_data = pd.read_csv(doc_name+str(id_bot)+'.csv')
          columns = df_data.columns
          data = df_data.values
        except:
          report_file = open("report_file_"+str(id_bot)+".txt", "a")
          report_file.write(f'[Session  {n_session}] | CSV nor found: {datetime.now()} \n')
          report_file.close()
          ## GET TABLE HEADER
          table_header = driver.find_element(By.ID,'j_idt24:frmPersonasCert:tblDatosPersonas_head')
          row_data = table_header.find_elements(By.TAG_NAME,'tr')
          for row in row_data:
            cell = row.find_elements(By.TAG_NAME,'th')
            columns = [val.text for val in cell]
          columns = np.array(columns)
          data = np.zeros((0,columns.shape[0])) 
        
          # CONFIRM SUCCESSFUL LOAD
        try_load = False
      except Exception:
        e = sys.exc_info()[1]
        print("Error data: ",e.args[0])
        n_session_falied += 1
        try_load = True
        driver.quit() 

      

      #col_report = ['ID','Bot_Name','Start','End','Time_Start_Page','Time_Start_Mining','Sessions','Time_S','Falied_Sessions','Time_FS','Expired_Sessions','Time_ES']

      #WRITE REPORT
      report_file = open("report_file_"+str(id_bot)+".txt", "r")
      report_lines = report_file.readlines()
      report_file.close()

      report_lines[-1] = f'Sessions: {n_session} | Falied sessions : {n_session_falied}| Expired sessions: {n_expired_sessions}\n'

      report_file = open("report_file_"+str(id_bot)+".txt", "w")
      report_file.writelines(report_lines)
      report_file.close()

    
    expired_session = False
    page_number=0
    page_checkpoint = start_page
    

    try:
      #Read the last page mined
      cache_file = open('cache_file'+str(id_bot)+'.txt')
      page_checkpoint = int(cache_file.readlines()[1].split(' ')[1])
      cache_file.close()
    except:
      cache_file = open('cache_file'+str(id_bot)+'.txt', "w")
      cache_lines = f'Shape_data: (0, 0) \nPage: 0 \nStar_Page: 0 \nEnd_Page: 0'
      cache_file.writelines(cache_lines)
      cache_file.close()
    print('page_checkpoint: ',page_checkpoint) 
    # ------------  CONTROL ----------- #

    #WRITE REPORT
    report_file = open("report_file_"+str(id_bot)+".txt", "a")
    report_file.write('===================================================================== \n')
    report_file.write(f'[Session  {n_session}] | Go to last mined page: {datetime.now()} \n')
    report_file.close()
    
    naeip = 0
    #print('BOT N: ',id_bot,' Start: ',start_page,' End: ',final_page)
    #print('page_number: ',page_number,' page_checkpoint: ',page_checkpoint) 
    while(page_number<page_checkpoint and page_number<final_page and not expired_session):
        try:
            page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
            page_number = int(page_active[1].text)
            #sleep(2)

            if (page_number>=final_page):
                print('Final page: '+str(page_number))
                break
            
            btn_siguiente = driver.find_element(By.XPATH, '//*[@id="j_idt24:frmPersonasCert:tblDatosPersonas_paginator_bottom"]/a[3]')

            if (page_checkpoint - page_number)>10:
              btn_siguiente = driver.find_element(By.XPATH,'//*[@id="j_idt24:frmPersonasCert:tblDatosPersonas_paginator_bottom"]/span/a[10]')
                
            btn_siguiente.click()
        except:
          if (page_number>=final_page):
                print('Error and Final page: '+str(page_number))
                break
          elif (naeip>10):
            try:
              #Check Expired session
                span_expired_session = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
                if 'Sesión Caducada' == span_expired_session.text or 'Excepción Grave' == span_expired_session.text:
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
          
    
    if (page_number >= final_page):
      print('END PROGRAM')
      go = False
      break
    
    elif(not expired_session):

      #WRITE REPORT
      report_file = open("report_file_"+str(id_bot)+".txt", "a")
      report_file.write(f'[Session  {n_session}] | Resume Mining since page {page_number} : {datetime.now()} \n')
      report_file.close()



      #WRITE REPORT
      report_file = open("report_file_"+str(id_bot)+".txt", "a")
      report_file.write(f'[Session  {n_session}] | Start to Mining: {datetime.now()} \n')
      report_file.close()

      #TIME CONTROL
      time_end_page = time()
      tiempo_data_inicio = time()

      condition = True

      while(condition):

        try_load = True

        # naeip = Number of Attempts to Extract Information from the Page
        naeip = 0

        while(try_load):
          try:
            table_data = driver.find_element(By.ID,'j_idt24:frmPersonasCert:tblDatosPersonas_data')
            row_data = table_data.find_elements(By.TAG_NAME,'tr')
            table_page = []
            for row in row_data:
              cell = row.find_elements(By.TAG_NAME,'td')
              reg = [val.text for val in cell]
              table_page.append(reg)
            data=np.append(data,np.array(table_page),axis=0)
            try_load = False
          
          except: 
            if naeip>=10:
              try:
                #Check Expired session
                  span_expired_session = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
                  if 'Sesión Caducada' == span_expired_session.text or 'Excepción Grave' == span_expired_session.text:
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
          #NEXT PAGE
          driver.implicitly_wait(30)
          btn_siguiente = driver.find_element(By.XPATH, '//*[@id="j_idt24:frmPersonasCert:tblDatosPersonas_paginator_bottom"]/a[3]')
          page_active = driver.find_elements(By.CLASS_NAME,'ui-state-active')
          page_number = int(page_active[1].text)
          driver.implicitly_wait(30)
          tiempo_data_fin = time()
          
          # SAVE INFORMATION ABOUT MINING
          save_info(id_bot,doc_name,data,columns,tiempo_data_inicio,tiempo_data_fin,page_number,start_page,final_page)

          if (page_number>=final_page):
            go = False
            break

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

      save_info(id_bot,doc_name,data,columns,tiempo_data_inicio,tiempo_data_fin,page_number,start_page,final_page)

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

    N_BOT_DRIVERS = 1
    QUERY_INPUT = ' '
    DOC_NAME = 'pcoec'
    start_page_list = []
    final_page_list = []
    final_page = 0

    options = webdriver.ChromeOptions() 
    # to supress the error messages/logs

    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    #options.add_argument('--headless')
    #options.add_argument('--no-sandbox')
    #options.add_argument('--disable-dev-shm-usage')

    doc_exist = False
    
    try:
        botnet_report_file = open("botnet_report_"+DOC_NAME+".txt", "r")
        lines_botnet_report_file = botnet_report_file.readlines()
        final_page = int(lines_botnet_report_file[0].split(' ')[1])
        N_BOT_DRIVERS = int(lines_botnet_report_file[1].split(' ')[1])
        botnet_report_file.close()
        doc_exist = True
    except:
        final_page = get_final_page(options,QUERY_INPUT)
    

    range_page = final_page//N_BOT_DRIVERS
    mark_page = 0
    for i in range(N_BOT_DRIVERS):
        start_page_list.append(mark_page)
        mark_page+=range_page
        if(mark_page>final_page or i >= N_BOT_DRIVERS -1 ):
          final_page_list.append(final_page+1)
        else:
          final_page_list.append(mark_page)


    print('final_page: ',final_page)
    print('N_BOT_DRIVERS: ',N_BOT_DRIVERS)
    print('START: ',start_page_list)
    print('FINAL: ',final_page_list)
    
    if not doc_exist:
        print('New Botnet Report File')
        botnet_report_file = open("botnet_report_"+DOC_NAME+".txt", "w")
        botnet_report_file.write('PAGES: '+str(final_page)+'\n')
        botnet_report_file.write('N_BOT_DRIVERS: '+str(N_BOT_DRIVERS)+'\n')
        botnet_report_file.write('START_PAGE_LIST: '+str(start_page_list)+'\n')
        botnet_report_file.write('FINAL_PAGE_LIST: '+str(final_page_list)+'\n')
        botnet_report_file.close()


    #webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

    bot(1,DOC_NAME,options,4386,8772,QUERY_INPUT)
'''
    for i in range(N_BOT_DRIVERS):
        browserThread = threading.Thread(target=bot,args=(i,DOC_NAME,options,start_page_list[i],final_page_list[i],QUERY_INPUT))
        browserThread.start()
'''