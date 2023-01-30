try:
            table_data = driver.find_element(By.ID,'j_idt24:pnlOrganismo:tblDatosTabla_data')
            row_data = table_data.find_elements(By.TAG_NAME,'tr')
            table_page = np.zeros((0,17))
            i=0
            for row in row_data:
              
              print('fila: ',i)
              cell = row.find_elements(By.TAG_NAME,'td')
              reg = [val.text for val in cell]
              #Detail Button
              detail_button = cell[-1].find_element(By.TAG_NAME,'button')
              detail_button.click()
              
              ## GET TABLE HEADER
              if(header_cont <= 0):
                table_detail_header = driver.find_element(By.ID,'frmAreaEspecialidadOc:dataTable_head')
                row_detail_data = table_detail_header.find_element(By.TAG_NAME,'tr')
                cell_dh = row_detail_data.find_elements(By.TAG_NAME,'th')
                columns_d = [val_dh.text for val_dh in cell_dh]
                columns = np.append(columns,columns_d)
                header_cont+=1

              reg_complete = []
              condition_cc = True
              # Capacitación Continua
              while(condition_cc):
                
                try_load_cc = True

                # naeip = Number of Attempts to Extract Information from the Page
                naeip_cc = 0

                while(try_load_cc):
                  try:      
                    table_detail = driver.find_element(By.ID,'frmAreaEspecialidadOc:dataTable_data')
                    row_detail = table_detail.find_elements(By.TAG_NAME,'tr')
                    for row_d in row_detail:
                      cell_d = row_d.find_elements(By.TAG_NAME,'td')
                      reg_d = [val_d.text for val_d in cell_d]
                      reg_aux = reg+reg_d
                      reg_complete.append(reg_aux)

                    button_next_page_detail =  driver.find_element(By.XPATH,'//*[@id="frmAreaEspecialidadOc:dataTable_paginator_bottom"]/a[3]')
                    button_next_page_detail.click()
                  except Exception:  
                    condition_cc = False
                  break
                
              table_page=np.append(table_page,np.array(reg_complete),axis=0)
              print('table_page: ',table_page)
              button_exit = driver.find_element(By.XPATH,'//*[@id="frmAreaEspecialidadOc:j_idt231"]/div[1]/a')  
              button_exit.click()
              i+=1    
            
            sleep(1) 
            data=np.append(data,np.array(table_page),axis=0)
            print('Data: ',data)
             
            
            try_load = False
          
          except Exception: 
            e = sys.exc_info()[1]
            print("Error data: ",e.args[0])
            break
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