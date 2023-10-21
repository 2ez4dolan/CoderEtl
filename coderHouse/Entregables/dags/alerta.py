from clima import directorios
from datetime import datetime
import os 
import pandas as pd 
import smtplib
from airflow.configuration import conf


smtp_user = conf.get('smtp', 'smtp_user')
smtp_password = conf.get('smtp', 'smtp_password')

print(smtp_password)
print(smtp_user)
#Genero funcion para la poder elegir que ciudades quiero recibir o no las alertas
def opciones_alerta(df):
    opciones_alerta = {ciudad: True for ciudad in df['nombre']}
    
    ciudades_desactivar = ['Morón', 'Merlo', 'Belgrano']
    for ciudad in ciudades_desactivar:
        opciones_alerta[ciudad] = False
    
    return opciones_alerta
    

def alertaMinima():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    file_extraccion = os.path.join(directorios(), f'cargar_{fecha_actual}.csv')
    df = pd.read_csv(file_extraccion)
    
    ciudades= opciones_alerta(df)
    df_min = df[['nombre','temp_min']]
    
    for index, fila in df_min.iterrows():
        ciudad = fila['nombre']
        temperatura = fila['temp_min']

        if temperatura == 45 and ciudades.get(ciudad, False):
            try:
                x=smtplib.SMTP('smtp.gmail.com',587)
                x.starttls()
                x.login(smtp_user,smtp_password)
                subject='Alerta de temperaturas Bajas'
                body_text=f'Atencion : posibles temperaturas bajas en su area , temp :{temperatura}'
                message='Subject: {}\n\n{}'.format(subject,body_text)
                x.sendmail(smtp_user,'dolangp1@gmail.com',message)
                print('Exito')
            except Exception as exception:
                print(exception)
                print('Failure')
        else :
            print(f"no hay alerta , temp_min = {temperatura}, ciudad:{ciudad}")


def alertaMaxima():
    fecha_actual= datetime.today().strftime("%Y%m%d")
    file_extraccion = os.path.join(directorios(), f'cargar_{fecha_actual}.csv')
    df = pd.read_csv(file_extraccion)
    
    ciudades= opciones_alerta(df)
    df_min = df[['nombre','temp_max']]
    
    for index, fila in df_min.iterrows():
        ciudad = fila['nombre']
        temperatura = fila['temp_min']

        if temperatura == 0 and ciudades.get(ciudad, False):
            try:
                x=smtplib.SMTP('smtp.gmail.com',587)
                x.starttls()
                x.login(smtp_user,smtp_password)
                subject='Alerta de temperaturas Bajas'
                body_text=f'Atencion : posibles temperaturas muy altas  en su area , temp :{temperatura} ,  hidratarse y mantenerse bajo sombra'
                message='Subject: {}\n\n{}'.format(subject,body_text)
                x.sendmail(smtp_user,'dolangp1@gmail.com',message)
                print('Exito')
            except Exception as exception:
                print(exception)
                print('Failure')
        else :
            print(f"no hay alerta , temp_min = {temperatura}, ciudad:{ciudad}")