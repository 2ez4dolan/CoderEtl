import psycopg2


conn= psycopg2.connect(
        host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
        dbname="data-engineer-database",
        user="dolangp1_coderhouse",
        password="YId4w92C3O",
        port="5439"
) 
conn.autocommit=True


def crear_tabla():
    cur= conn.cursor() 
    query= "CREATE TABLE IF NOT EXISTS Clima (clima_id int ,nombre varchar(255),pais varchar(255),descripcion varchar(255),temp decimal(5,2),feels_like decimal(5,2),temp_max decimal(5,2), temp_min decimal(5,2),humedad decimal(5),fecha_solicitud date, PRIMARY KEY (clima_id, nombre, fecha_solicitud))"
    cur.execute(query)
    print("tabla creada")

def insertar(id,nombre,pais,descripcion,temp,sensacion,temp_max,temp_min,humedad,fecha_actual):
    
    cur=conn.cursor()
    query_consulta= f"select clima_id from Clima where nombre ='{nombre}' and fecha_solicitud='{fecha_actual}'"
    cur.execute(query_consulta)
    valor_existente = cur.fetchone()    
    if  valor_existente:
        print("Ya existe el registro en la base")
    else :
        insert = f"INSERT INTO Clima(clima_id, nombre, pais, descripcion, temp, feels_like, temp_max, temp_min, humedad,fecha_solicitud)VALUES ({id}, '{nombre}', '{pais}', '{descripcion}', {temp}, {sensacion}, {temp_max}, {temp_min}, {humedad},'{fecha_actual}')"
        cur.execute(insert)
        print("inserccion realizada")
    

def cerrar():
    conn.close()
    print("conexion cerrada")