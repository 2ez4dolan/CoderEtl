import psycopg2


conn= psycopg2.connect(
        host="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
        dbname="data-engineer-database",
        user="dolangp1_coderhouse",
        password="YId4w92C3O",
        port="5439"
) 
conn.autocommit=True

cur= conn.cursor() 
query= "CREATE TABLE IF NOT EXISTS Clima (clima_id int PRIMARY KEY ,nombre varchar(255),pais varchar(255),descripcion varchar(255),temp decimal(2,2),feels_like decimal(2,2),temp_max decimal(2,2), temp_min decimal(2,2),humedad decimal(3),fecha_solicitud date) "
cur.execute(query)


conn.close()