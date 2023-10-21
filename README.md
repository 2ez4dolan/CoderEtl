# CoderEtl
tp entregable de coderhouse

Pipeline de API del Clima

Este proyecto es un pipeline de una API del clima que realiza extracción, transformación, carga y envío de alertas. Todo el proceso se ejecuta en un DAG (Directed Acyclic Graph) en Apache Airflow, que consta de 5 tareas interconectadas.

Estructura del Proyecto

dags/: Contiene los archivos DAG de Apache Airflow.

config/: Contiene archivos de configuración, como airflow.cfg.

Instrucciones de Uso

Para ejecutar el proyecto, sigue estos pasos:

Clona este repositorio en tu máquina local.

Asegúrate de tener Docker y Docker Compose instalados.

Configura las variables de entorno y las credenciales necesarias en config/airflow.cfg.

Ejecuta docker-compose up en el directorio raíz del proyecto para iniciar el entorno de Apache Airflow.

Accede al dashboard de Airflow en tu navegador.

La api_key esta guardada dentro del dashboard de Airflow

Programación de Tareas

El proyecto incluye un DAG que consta de las siguientes tareas:

Extracción de la API: Esta tarea extrae datos de la API del clima.

Transformación de Datos: Realiza la transformación de los datos, como la conversión de grados Kelvin a Celsius.

Carga de Datos: Carga los datos transformados en una base de datos, como Redshift.

Alertas de Temperatura: Envía alertas por correo electrónico cuando se superan umbrales de temperatura específicos.
