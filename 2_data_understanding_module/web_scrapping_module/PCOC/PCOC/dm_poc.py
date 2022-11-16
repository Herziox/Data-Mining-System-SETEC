#Libraries
from multiprocessing import Condition
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from time import time
import numpy as np
import os

from datetime import datetime


def save_info(data,columns,tds,tde,page):
    poc_df = pd.DataFrame(data=data,columns=columns)
    poc_df.to_csv('poc.csv')


    fichero = open('cache.txt')
    time_work = float(fichero.readlines()[3].split(' ')[1])
    fichero.close()
    

    file = open("cache.txt", "w")
    file.write('Shape data: '+str(poc_df.shape)+'\n')
    file.write('Page: '+str(page+1)+'\n')

    file.write('========== Time ========='+'\n')

    time_total = tde-tds
    time_work += time_total
    file.write('Time: '+str(time_work)+'\n')

    hours, rem = divmod(time_total,3600)
    minutes, seconds = divmod(rem, 60)
    file.write('Last session time: '+"{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)+'\n')

    hours, rem = divmod(time_work,3600)
    minutes, seconds = divmod(rem, 60)
    file.write('Total time: '+"{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)+'\n')
    file.close()



#Develop
report_file = open("report.txt", "a")
report_file.write('####################--------START PROGRAM---------####################  \n')
report_file.write('####################  '+ str(datetime.now()) +'  #################### \n')
report_file.write('===================================================================== \n')
report_file.write('===================================================================== \n')
report_file.close()

go = True

sesion = 0

n_sesion = 0

n_sesion_falied = 0

n_expired_sesions = 0

QUERY_INPUT = ' '

while(go and sesion<100):
  

  tiempo_pagina_inicio = time()

  try_load = True

  #Try to load page
  while(try_load):

    ## GET TABLE
    try:
      n_sesion += 1

      #Settings
      
      options = webdriver.ChromeOptions() 
      # to supress the error messages/logs
      options.add_experimental_option('excludeSwitches', ['enable-logging'])

      options.add_argument('--headless')
      options.add_argument('--no-sandbox')
      options.add_argument('--disable-dev-shm-usage')

      #driver = webdriver.Chrome(options=options,executable_path=r'C:\SeleniumDrivers\chromedriver.exe')
      driver = webdriver.Chrome(options=options,service=Service(ChromeDriverManager().install()))

      ## URL
      driver.get('http://portal.trabajo.gob.ec/setec-portal-web/pages/personasCapacitadasOperadores.jsf')

      ## SELECT FILTER
      driver.implicitly_wait(5)
      try:
        cmb_seleccione_filtro = driver.find_element(By.ID, 'j_idt24:frmCapIndPersonas:cmbSubOcIndependiente:cmbSubOcIndependiente')
        cmb_seleccione_filtro.click()
      except:
        print('No element with that name')


      ## SELECT OPTION
      driver.implicitly_wait(5)
      cmb_item_nombre_curso = driver.find_element(By.ID, 'j_idt24:frmCapIndPersonas:cmbSubOcIndependiente:cmbSubOcIndependiente_3')
      # print('Combo Box Item: ',cmb_item_nombre_curso.text)
      cmb_item_nombre_curso.click()

      ## WRITE QUERY
      driver.implicitly_wait(5)
      txt_nombre_curso_perfil = driver.find_element(By.ID, 'j_idt24:frmCapIndPersonas:txtParametroCapCurso:txtParametroCapCurso')
      txt_nombre_curso_perfil.click()
      driver.implicitly_wait(5)
      txt_nombre_curso_perfil.send_keys(QUERY_INPUT)


      ## SEARCH QUERY
      driver.implicitly_wait(5)
      btn_buscar = driver.find_element(By.ID, 'j_idt24:frmCapIndPersonas:j_idt48')
      btn_buscar.click()

    
      table_header = driver.find_element(By.TAG_NAME,'thead')
      row_data = table_header.find_elements(By.TAG_NAME,'tr')
      columns = []
      for row in row_data:
        cell = row.find_elements(By.TAG_NAME,'th')
        columns = [val.text for val in cell]
      try_load = False
    except:
      n_sesion_falied += 1
      try_load = True
      driver.quit() 

    report_file = open("report.txt", "r")
    report_lines = report_file.readlines()
    report_file.close()
    report_lines[-1] = f'Sessions: {n_sesion} | Falied sessions : {n_sesion_falied}| Expired sessions: {n_expired_sesions}\n'

    report_file = open("report.txt", "w")
    report_file.writelines(report_lines)
    report_file.close()

  expired_sesion = False

  columns = np.array(columns)

  exist_file = False

  page_number=0


  #Read the last page mined
  fichero = open('cache.txt')
  page_str = str(fichero.readlines()[1].split(' ')[1])
  fichero.close()

  page_checkpoint = int(page_str)

  final_page = False

  btn_control = driver.find_element(By.XPATH,'//*[@id="j_idt24:frmCapIndPersonas:tbl4_paginator_bottom"]/a[4]')
  driver.execute_script('arguments[0].click()',btn_control)
  sleep(0.5)


  stop = driver.find_elements(By.CLASS_NAME,'ui-state-active')
  last_page_number = int(stop[1].text)
  #last_page_number = 10000


  btn_control = driver.find_element(By.XPATH,'//*[@id="j_idt24:frmCapIndPersonas:tbl4_paginator_bottom"]/a[1]')
  driver.execute_script('arguments[0].click()',btn_control)
  sleep(0.5)

  stop = driver.find_elements(By.CLASS_NAME,'ui-state-active')
  page_number = int(stop[1].text)

  report_file = open("report.txt", "a")
  report_file.write('===================================================================== \n')
  report_file.write(f'[Session  {n_sesion}] | Go to last mined page: {datetime.now()} \n')
  report_file.close()
  naeip = 0
  
  

  
  while(page_number<=page_checkpoint-1 and not final_page and not expired_sesion):
      
      try:
          stop = driver.find_elements(By.CLASS_NAME,'ui-state-active')
          page_number = int(stop[1].text)

          if (page_number>=last_page_number):
              print('Final page: '+str(page_number))
              final_page = True
              break
          
          btn_siguiente = driver.find_element(By.XPATH, '//*[@id="j_idt24:frmCapIndPersonas:tbl4_paginator_bottom"]/a[3]')

          if (page_checkpoint - page_number)>5:
            btn_siguiente = driver.find_element(By.XPATH,'//*[@id="j_idt24:frmCapIndPersonas:tbl4_paginator_bottom"]/span/a[10]')
            
              
          btn_siguiente.click()
      except:
        if (page_number>=last_page_number):
              print('Error and Final page: '+str(page_number))
              final_page = True
        elif (naeip>10):
          try:
            #Check Expired Sesion
              span_expired_sesion = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
              if 'Sesión Caducada' == span_expired_sesion.text:
                n_expired_sesions +=1
                expired_sesion = True
                report_file = open("report.txt", "a")
                report_file.write(f'[Session  {n_sesion}] | Expired: {datetime.now()} \n')
                report_file.close()
                break
          except:
              naeip = 0
        else:
          naeip += 1
        
  
  if not expired_sesion and not final_page:

    report_file = open("report.txt", "a")
    report_file.write(f'[Session  {n_sesion}] | Resume Mining since page {page_number} : {datetime.now()} \n')
    report_file.close()
    data = np.zeros((0,columns.shape[0]))
    try:
      df_data = pd.read_csv('poc.csv')
      df_data = df_data.drop(['Unnamed: 0'], axis=1)
      data = df_data.values
    except:
      report_file = open("report.txt", "a")
      report_file.write(f'[Session  {n_sesion}] | CSV nor found: {datetime.now()} \n')
      report_file.close()


    condition = True


    tiempo_pagina_fin = time()


    report_file = open("report.txt", "a")
    report_file.write(f'[Session  {n_sesion}] | Start to Mining: {datetime.now()} \n')
    report_file.close()


    tiempo_data_inicio = time()

    while(condition):

      flat = True

      # naeip = Mumber of Attempts to Extract Information from the Page
      naeip = 0

      while(flat):
        try:
          table_data = driver.find_element(By.TAG_NAME,'tbody')
          row_data = table_data.find_elements(By.TAG_NAME,'tr')
          table_page = []
          for row in row_data:
            cell = row.find_elements(By.TAG_NAME,'td')
            reg = [val.text for val in cell]
            table_page.append(reg)
          data=np.append(data,np.array(table_page),axis=0)
          flat = False
        
        except:
          flat = True
          naeip += 1
          #print('Nuevo intento de ciclo',i,' Nº: ',j,'\n')
          
        if naeip>=10:
          try:
            #Check Expired Sesion
              span_expired_sesion = driver.find_element(By.XPATH, '/html/body/div[2]/div/span[3]')
              if 'Sesión Caducada' == span_expired_sesion.text:
                n_expired_sesions +=1
                expired_sesion = True
                report_file = open("report.txt", "a")
                report_file.write(f'[Session  {n_sesion}] | Expired: {datetime.now()} \n')
                report_file.close()
                break
          except:
              naeip = 0


      if expired_sesion:
        report_file = open("report.txt", "a")
        report_file.write(f'[Session  {n_sesion}] | Expired: {datetime.now()} \n')
        report_file.close()
        break
      
      #NEXT PAGE

      driver.implicitly_wait(30)

      btn_siguiente = driver.find_element(By.XPATH, '//*[@id="j_idt24:frmCapIndPersonas:tbl4_paginator_bottom"]/a[3]')
      
      if btn_siguiente.is_enabled:
        try:
          stop = driver.find_elements(By.CLASS_NAME,'ui-state-active')
          page_number = int(stop[1].text)
          driver.implicitly_wait(30)
          btn_siguiente.click()
          tiempo_data_fin = time()
          
          # SAVE INFORMATION ABOUT MINING
          save_info(data,columns,tiempo_data_inicio,tiempo_data_fin,page_number) 
        except:
          condition = False
          report_file = open("report.txt", "a")
          report_file.write('========================= Fail click ===========================\n')
          report_file.close()
          break
        
      else:
        print('Disable button') 
        condition = False
        break
      #print('Numero de filas: ',len(data), '\n')
      

    tiempo_data_fin = time()

    report_file = open("report.txt", "a")
    report_file.write(f'[Session  {n_sesion}] | End Session: {datetime.now()} \n')
    report_file.close()

    save_info(data,columns,tiempo_data_inicio,tiempo_data_fin,page_number)

    if(page_number<last_page_number and sesion<100):
      go = True
      sesion += 1
    else:
      go = False
      break
    driver.quit()
  driver.quit()
report_file = open("report.txt", "a")
report_file.write('############################ END PROGRAM ############################+\n')
report_file.close()


    