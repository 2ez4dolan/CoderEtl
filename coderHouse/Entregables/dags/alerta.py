from clima import directorios
from datetime import datetime
import os 
import pandas as pd 
import smtplib
from airflow.configuration import conf
from email.mime.text import MIMEText


smtp_user = conf.get('smtp', 'smtp_user')
smtp_password = conf.get('smtp', 'smtp_password')


#Genero funcion para la poder elegir que ciudades quiero recibir o no las alertas
def opciones_alerta(df):
    opciones_alerta = {ciudad: True for ciudad in df['nombre']}
    
    ciudades_desactivar = ['Morón', 'Merlo', 'Belgrano']
    for ciudad in ciudades_desactivar:
        opciones_alerta[ciudad] = False
    
    return opciones_alerta
    

#Funcion para envio de email 
def envio_email(alertas, asunto):
    try:
        x = smtplib.SMTP('smtp.gmail.com', 587)
        x.starttls()
        x.login(smtp_user, smtp_password)
        body_text = '\n'.join(alertas)
        message = MIMEText(body_text, 'plain', 'utf-8')
        message['Subject'] = asunto
        message['From'] = smtp_user
        message['To'] = 'dolangp1@gmail.com'  
        x.sendmail(smtp_user, 'dolangp1@gmail.com', message.as_string())
        print('Correo enviado con éxito')
    except Exception as exception:
        print(exception)
        print('Error al enviar el correo')
       
       

#Filtros para el envio de alertas 
def AlertaTemperatura(valor,asunto):
    fecha_actual= datetime.today().strftime("%Y%m%d")
    file_extraccion = os.path.join(directorios(), f'cargar_{fecha_actual}.csv')
    df = pd.read_csv(file_extraccion)

    ciudades= opciones_alerta(df)
    df = df[['nombre',valor]]
    
    alertas_min=[]
    alertas_max=[]

    for index, fila in df.iterrows():
        ciudad = fila['nombre']
        temperatura = fila[valor]

        if (valor =='temp_min' and temperatura <= 7 and ciudades.get(ciudad, False)):
            alertas_min.append(f"Alerta: {ciudad} tiene una temperatura minima de {temperatura}°C.")
        
        if  (valor =='temp_max' and temperatura>= 10 and ciudades.get(ciudad, False)):
             alertas_max.append(f"Alerta: {ciudad} tiene una temperatura maxima de {temperatura}°C.")
        
    if(alertas_min):
        envio_email(alertas_min,asunto)

    if(alertas_max):
        envio_email(alertas_max,asunto)
            



def alertaMinima():
    
    asunto= 'Alertas de temperaturas Bajas'
    temp= 'temp_min'

    AlertaTemperatura(temp,asunto)




def alertaMaxima():
 
    asunto= 'Alertas de Altas temperatura'
    temp= 'temp_max'

    AlertaTemperatura(temp,asunto)
          