#remotes::install_github("lawine90/datagokR")

library(remotes)
library(rvest) # Web Scrapping Library
library(tidyverse) # Easy load packages
library(readr)
library(httr)
library("xml2")
library(datagokR)


datacsv <- read_csv("data/data_query_poc.csv")
View(datacsv)
personasdf <- data.frame()
data2 <- datacsv
print(nrow(data2))
#for(i in 1:nrow(data2)) {       # for-loop over rows
for(i in 1:nrow(data2) ){       # for-loop over rows
 # data2[i, ] <- data2[i, ] - 100
  dataci = data2[i, 1]$`Número Documento`
  datanombre=data2[i, 2]$`Apellidos / Nombres...2`
  datanombre=gsub(" ", "-",datanombre)
  persona <- data.frame() 
  
   print(i)
   # print(nrow(data2))
   # print(dataci)
   # print(datanombre)
  url=paste("https://rucecuador.com/rucsri/",datanombre,"-",dataci,"001",sep="")
  print(url)
  #cat(i,url)
  
  #data <- read_html("https://rucecuador.com/rucsri/jimenez-torres-darwin-francisco-1103481337001")
 
  # Allow 10 seconds
  tryCatch(data <- url %>% GET(., timeout(10)) %>% read_html,
           error = function(e) { 
             print("Error in get html, Timed out 10 seconds, we go on ! i =")
             print(i)
             } )
  
  #data <- url %>% GET(., timeout(10)) %>% read_html
  
  #data <- read_html(url)
  #pg <- read_html(data)

  data %>% 
    html_nodes(xpath = '//*[@id="inmuebledesc"]/div[1]/div[2]') %>%
    xml_attr("value")
  
  #install.packages("remotes")

  #remotes::install_github("muschellij2/processVISION")

  razonSocial <- xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[1]/div[2]/text()'))
  if (identical(razonSocial, character(0))) razonSocial = ""
  
  ruc <-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[2]/div[2]/text()'))
  if (identical(ruc, character(0))) ruc = ""
  
  nombreComercial <-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[3]/div[2]/text()'))
  if (identical(nombreComercial, character(0))) nombreComercial = ""
  
  claseContribuyente<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[4]/div[2]/text()'))
  if (identical(claseContribuyente, character(0))) claseContribuyente = ""
  
  estadoContribuyente<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[5]/div[2]/text()'))
  if (identical(estadoContribuyente, character(0))) estadoContribuyente = ""
  
  
  fechaActualizacion<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[6]/div[2]/text()'))
  if (identical(fechaActualizacion, character(0))) fechaActualizacion = ""
  
  
  
  fechainicioactividades<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[7]/div[2]/text()'))
  if (identical(fechainicioactividades, character(0))) fechainicioactividades = ""
  
  fechaSuspensionDefinitiva<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[8]/div[2]/text()'))
  if (identical(fechaSuspensionDefinitiva, character(0))) fechaSuspensionDefinitiva = ""
  
  fechaReinicioActividades<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[9]/div[2]/text()'))
  if (identical(fechaReinicioActividades, character(0))) fechaReinicioActividades = ""
  
  
  
  tipoContribuyente<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[10]/div[2]/text()'))
  if (identical(tipoContribuyente, character(0))) tipoContribuyente = ""
  
  obligadocontabilidad<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[11]/div[2]/text()'))
  if (identical(obligadocontabilidad, character(0))) obligadocontabilidad = ""
  
  
  sector<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[12]/div[2]/text()'))
  if (identical(sector, character(0))) sector = ""
  
  cedula<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[13]/div[2]/text()'))
  if (identical(cedula, character(0))) cedula = ""
  
  
  estadoactual<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[14]/div[2]/text()'))
  if (identical(estadoactual, character(0))) estadoactual = ""
  
  direccionprincipal<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[15]/div[2]/text()'))
  if (identical(direccionprincipal, character(0))) direccionprincipal = ""
  
  email<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[16]/div[2]/text()'))
  if (identical(email, character(0))) email = ""
  
  telefonocelular<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[17]/div[2]/text()'))
  if (identical(telefonocelular, character(0))) telefonocelular = ""
  
  telefonoconvencional<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[18]/div[2]/text()'))
  if (identical(telefonoconvencional, character(0))) telefonoconvencional = ""
  
  provincia<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[19]/div[2]/text()'))
  if (identical(provincia, character(0))) provincia = ""
  
  canton<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[20]/div[2]/text()'))
  if (identical(canton, character(0))) canton = ""
  
  parroquia<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[21]/div[2]/text()'))
  if (identical(parroquia, character(0))) parroquia = ""
  
  codigoCIIU<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[22]/div[2]/text()'))
  if (identical(codigoCIIU, character(0))) codigoCIIU = ""
  
  actividadEconomicaPrincipal<-  xml_text(html_nodes(data,xpath = '//*[@id="inmuebledesc"]/div[24]/div[2]/p/text()'))
  if (identical(actividadEconomicaPrincipal, character(0))) actividadEconomicaPrincipal = ""
  
  
  persona <- data.frame()
  persona <- data.frame(
            i,
            razonSocial,
            ruc,
            nombreComercial,
            claseContribuyente,
            estadoContribuyente,
            fechaActualizacion,
            fechainicioactividades,
            fechaSuspensionDefinitiva,
            fechaReinicioActividades,
            tipoContribuyente,
            obligadocontabilidad,
            sector,
            cedula,
            estadoactual,
            direccionprincipal,
            email,
            telefonocelular,
            telefonoconvencional,
            provincia,
            canton,
            parroquia,
            codigoCIIU,
            actividadEconomicaPrincipal
          )
  
  personasdf <- rbind(personasdf,persona)
  #View(razonSocial)
  head(personasdf)
  if (i %% 500 == 0){
    write_csv(personasdf, ("data/contact_data_poc_v1.csv"))
  }
  
}
write_csv(personasdf, ("data/contact_data_poc_v1.csv"))
View(personasdf)


