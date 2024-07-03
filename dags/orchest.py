# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime
from time import sleep
from random import choice
import decimal
## Librerias de base de datos
import pyodbc
import sqlalchemy as sa
from sqlalchemy.dialects.oracle import TIMESTAMP
import urllib
## Librerias de kafka
from kafka import KafkaProducer
from confluent_kafka.serialization import StringSerializer
from uuid import uuid4
# Admin - CREA EL TOPIC
from kafka.admin import KafkaAdminClient,NewTopic
import time
import requests
import json
import os
import re

######################################################### FUNCIONES DE AYUDA
## CONEXIÓN A LA BASE DE DATOS
def sql_server_connect():
    server = '10.218.1.118\master,1433' # to specify an alternate port
    database = 'datafeeds' 
    username = 'anova' 
    password = 'M0v1l1d4d2024'
    to_engine: str = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    cnxn = pyodbc.connect(to_engine)
    # query = "SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_INCIDENTES;"
    # df = pd.read_sql(query, cnxn)
    cursor = cnxn.cursor()
    return cursor, cnxn
## SERIALIZACION DE DATOS
def serialize_special_types(value):
    if isinstance(value, datetime):
        # return value.isoformat()
        return value.timestamp() * 1000 #- 3600000*5 ## Ajuste de zona horaria UTC to Bogota
    elif isinstance(value, decimal.Decimal):
        return float(value)
    else:
        return value
## CREA TOPIC
def crear_topic(topic):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers="broker:29092", client_id='test')
        topic_list = []
        new_topic = NewTopic(name=topic, num_partitions= 1, replication_factor=1)
        topic_list.append(new_topic)
        admin_client.create_topics(new_topics=topic_list)
        print(topic+" creado con exito")
    except:
        print(topic+" ya existe o no pudo ser creado")
## CREA CONECTOR
def crear_conector(properties_file):
        # Inicializa el cliente administrativo
        path = os.getcwd()
        print(path)
        f = os.path.join(path, properties_file)
        print(f)
        if os.path.exists(f):
            with open(f, 'r') as file:
                properties = json.load(file)
            try:
                # response = requests.post('http://localhost:8083/connectors', headers={'Content-Type': 'application/json'}, data=json.dumps(properties))
                response = requests.post('http://connect:8083/connectors', headers={'Content-Type': 'application/json'}, data=json.dumps(properties))
                if response.status_code == 201:
                    print(f"Sink connector {properties['name']} created successfully.")
                else:
                    print(f"Failed to create sink connector {properties['name']}. Error: {response.content}")
            except:
                print(f"Failed to create sink connector {properties['name']}. Error: {response.content}")
        else:
            print('Ruta no encontrada')
############################################### FUNCIONES DE TAREAS PRINCIPALES
## Tarea para creacion de topics y de conectores
def start_adm():
    ## Creación de topics
    crear_topic("nuse_mvincidents")
    crear_topic("nuse_mvcloned")
    crear_topic("nuse_mvunits")
    ## Tiempo para creación de topics
    time.sleep(3)
    ## Creación de conectores
    crear_conector("/opt/airflow/dags/config_conectors/connector_JdbcSinkConnector_MV_INCIDENTS_config.json")
    crear_conector("/opt/airflow/dags/config_conectors/connector_JdbcSinkConnector_MV_UNITS_config.json")
    crear_conector("/opt/airflow/dags/config_conectors/connector_JdbcSinkConnector_MV_CLONED_config.json")

## Tarea para Extraer y producir datos en topic INCIDENTES
def task_produce_inc():
    ## PRODUCER FUNCIONAL
    topic = "nuse_mvincidents"
    cursor, cnxn = sql_server_connect()
    kafka_server = ["broker:29092"]
    schema = {"schema": {
        "type": "struct",
        "fields": [
        {"optional": True, "type": "string","field": "INCIDENTNUMBER"}, #1-NUMERO_INCIDENTE (Corregir)
        {"optional": True, "type": "string","field": "DIRECCION"}, #2-DIRECCION
        {"optional": True, "type": "string","field": "DESCIRPCION_DIRECCION"}, #3-DESCRIPCIÓN_DIRECCIÓN (Corregir)
        {"optional": True, "type": "string","field": "NOMBRE_SITIO"}, #4-NOMBRE_SITIO
        {"optional": True, "type": "string","field": "SUBDIVISION"}, #5-SUBDIVISION
        {"optional": True, "type": "string","field": "LOCALIDAD"}, #6-LOCALIDAD
        {"optional": True, "type": "float","field": "LATITUDE"}, #7-COORDENADAX (Corregir)
        {"optional": True, "type": "float","field": "LONGITUDE"}, #8-COORDENADAY (Corregir)
        {"optional": True, "type": "int64","field": "INCIDENTDATE"}, #9-FECHA_CREACION (Corregir)
        {"optional": True, "type": "string","field": "NOMBRE_ESTADOINC"}, #10-NOMBRE_ESTADOINC
        {"optional": True, "type": "string","field": "ESTADO_INCIDENTE"}, #11-ESTADO_INCIDENTE
        {"optional": True, "type": "string","field": "LLAVE_EDITOR"}, #12-LLAVE_EDITOR
        {"optional": True, "type": "string","field": "USUARIO_EDITOR"}, #13-USUARIO_EDITOR
        {"optional": True, "type": "string","field": "TIPO_INCIDENTE_INICIAL"}, #14-TIPO_INICIDENTE_INICIAL  (Corregir)
        {"optional": True, "type": "string","field": "NOMBRE_TIPO_INCIDENTE_INICIAL"}, #15-NOMBRE_TIPO_INICIDENTE_INICIAL (Corregir)
        {"optional": True, "type": "string","field": "DESCR_TIPO_INCIDENTE_INICIAL"}, #16-DESCR_TIPO_INICIDENTE_INICIAL (Corregir)
        {"optional": True, "type": "string","field": "CIRCUNSTANCIAMOD_INICIAL"}, #17-CIRCUNSTANCIAMOD_INICIAL
        {"optional": True, "type": "string","field": "DESC_CIRCUNSTANCIAMOD_INICIAL"}, #18-DESCR_CIRCUNSTANCIAMOD_INICIAL (Corregir)
        {"optional": True, "type": "string","field": "TIPO_INCIDENTE"}, #19-TIPO_INCIDENTE
        {"optional": True, "type": "string","field": "NOMBRE_TIPO_INCIDENTE"}, #20-NOMBRE_TIPO_INCIDENTE
        {"optional": True, "type": "string","field": "DESCRIPCION_TIPO_INCIDENTE"}, #21-DESCRIPCION_TIPO_INCIDENTE
        {"optional": True, "type": "string","field": "CIRCUNSTANCIA_MODIFICADORA"}, #22-CIRCUNSTANCIA_MODIFICADORA
        {"optional": True, "type": "string","field": "DESCRIPCION_CIRCUNSTANCIAMOD"}, #23-DESCRIPCIÓN_CIRCUNSTANCIAMOD (Corregir)
        {"optional": True, "type": "string","field": "PRIORIDAD_INICIAL"}, #24-PRIORIDAD_INICIAL
        {"optional": True, "type": "string","field": "PRIORIDAD_ACTUAL"}, #25-PRIORIDAD_ACTUAL
        {"optional": True, "type": "int64","field": "ALARMA"}, #26-ALARMA
        {"optional": True, "type": "string","field": "ID_ALARMA"}, #27-ID_ALARMA
        {"optional": True, "type": "int64","field": "TIEMPO_ACTUALIZADO"}, #28-TIEMPO_ACTUALIZADO
        {"optional": True, "type": "int64","field": "LLAVE_AREA"}, #29-LLAVE_AREA
        {"optional": True, "type": "string","field": "AREA"}, #30-AREA
        {"optional": True, "type": "int64","field": "FECHA_INICIO_INCIDENTE"}, #31-FECHA_INICIO_INCIDENTE
        {"optional": True, "type": "string","field": "NOMBRE_LLAMANTE"}, #32-NOMBRE_LLAMANTE
        {"optional": True, "type": "string","field": "PRIMER_NOMBRE_LLAMANTE"}, #33-PRIMER_NOMBRE_LLAMANTE
        {"optional": True, "type": "string","field": "SEGUNDO_NOMBRE_LLAMANTE"}, #34-SEGUNDO_NOMBRE_LLAMANTE
        {"optional": True, "type": "string","field": "APELLIDO_LLAMANTE"}, #35-APELLIDO_LLAMANTE
        {"optional": True, "type": "string","field": "TELEFONO_LLAMANTE"}, #36-TELEFONO_LLAMANTE
        {"optional": True, "type": "string","field": "UNIDAD_PRIMARIA"}, #37-UNIDAD_PRIMARIA
        {"optional": True, "type": "string","field": "ID1_PRIMARIA"}, #38-ID1_PRIMARIA
        {"optional": True, "type": "string","field": "NOMBRE_PRIMARIA"}, #39-NOMBRE_PRIMARIA
        {"optional": True, "type": "string","field": "ID2_PRIMARIA"}, #40-ID2_PRIMARIA
        {"optional": True, "type": "string","field": "NOMBRE2_PRIMARIA"}, #41-NOMBRE2_PRIMARIA
        {"optional": True, "type": "int64","field": "TIEMPODESPACHO_PRIMERA_UNIDAD"}, #42-TIEMPODESPACHO_PRIMERA_UNIDAD
        {"optional": True, "type": "int64","field": "TIEMPOENRUTA_PRIMERA_UNIDAD"}, #43-TIEMPOENRUTA_PRIMERA_UNIDAD
        {"optional": True, "type": "int64","field": "TIEMPOLLEGADA_PRIMERA_UNIDAD"}, #44-TIEMPOLLEGADA_PRIMERA_UNIDAD
        {"optional": True, "type": "int64","field": "FECHA_CIERRE"}, #45-FECHA_CIERRE
        {"optional": True, "type": "int64","field": "CONTEO_REABIERTOS"}, #46-CONTEO_REABIERTOS
        {"optional": True, "type": "string","field": "AGENCIA"}, #47-AGENCIA
        {"optional": True, "type": "string","field": "CODIGO_DISPOSICION"}, #48-CODIGO_DISPOSICION
        {"optional": True, "type": "string","field": "COMENTARIOS"}, #49-COMENTARIOS
        {"optional": True, "type": "string","field": "INCIDENTDATE_DATE"}, #49-INCIDENTDATE_DATE
        {"optional": True, "type": "string","field": "INCIDENTDATE_TIME"}, #50-INCIDENTDATE_TIME
        {"optional": True, "type": "string","field": "FECHA_CIERRE_DATE"}, #51-FECHA_CIERRE_DATE
        {"optional": True, "type": "string","field": "FECHA_CIERRE_TIME"}, #52-FECHA_CIERRE_TIME
        {"optional": True, "type": "int64","field": "ID"}, #53-ID
        ],
        "name": "nuse_incidents_schema"
    }
    }
        
    p = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    try:
        # Ejecutar consulta en la base de datos
        # cursor.execute("SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_INCIDENTES;")
        cursor.execute("SELECT * FROM datafeeds.sch_movilidad.View_INTERFAZ_MOVILIDAD_INCIDENTES;")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        def corregir_noombres(x):
            if x=="NUMERO_INCIDENTE":
                r = 'INCIDENTNUMBER'
            # elif x=="DESCRIPCIÓN_DIRECCIÓN":
            #     r = 'DESCIRPCION_DIRECCION'
            elif x=="COORDENADAX":
                r = 'LONGITUDE'
            elif x=="COORDENADAY":
                r = 'LATITUDE'
            elif x=="FECHA_CREACION":
                r = 'INCIDENTDATE'
            elif x=="TIPO_INICIDENTE_INICIAL":
                r = 'TIPO_INCIDENTE_INICIAL'
            elif x=="NOMBRE_TIPO_INICIDENTE_INICIAL":
                r = 'NOMBRE_TIPO_INCIDENTE_INICIAL'
            elif x=="DESCR_TIPO_INICIDENTE_INICIAL":
                r = 'DESCR_TIPO_INCIDENTE_INICIAL'
            elif x=="DESCR_CIRCUNSTANCIAMOD_INICIAL":
                r = 'DESC_CIRCUNSTANCIAMOD_INICIAL'
            # elif x=="DESCRIPCIÓN_CIRCUNSTANCIAMOD":
            #     r = 'DESCRIPCION_CIRCUNSTANCIAMOD'
            else:
                r=x
            return r
        columns = list(map(corregir_noombres, columns))
        extras = ["INCIDENTDATE_DATE","INCIDENTDATE_TIME","FECHA_CIERRE_DATE","FECHA_CIERRE_TIME","ID"]
        columns.extend(extras)
        string_serializer = StringSerializer('utf_8')
        for row in rows:
            date_ini = row[8].strftime('%d/%m/%Y')
            time_ini = row[8].strftime('%H:%M:%S')
            date_fin = row[44].strftime('%d/%m/%Y') if row[44] is not None else ""
            time_fin = row[44].strftime('%H:%M:%S') if row[44] is not None else ""
            id = int(re.findall(r'-(\d+)-',row[0])[0])
            val = [serialize_special_types(value) for value in row]
            val.append(date_ini)
            val.append(time_ini)
            val.append(date_fin)
            val.append(time_fin)
            val.append(id)
            payload = dict(zip(columns, val))
            pl = {'payload': payload}
            # Publicar cada fila en el tópico de Kafka
            p.send(topic,key = string_serializer(str(uuid4())), value=schema | pl)
        p.flush()
    except:
        print("Error de inserción en la base de datos o extracción de la base de datos P1")
    finally:
        cursor.close()
        cnxn.close()

## Tarea para Extraer y producir datos en topic UNITS
def task_produce_units():
    topic="nuse_mvunits"
    cursor, cnxn = sql_server_connect()
    kafka_server = ["broker:29092"]
    schema = {"schema": {
        "type": "struct",
        "fields": [
        {"optional": True, "type": "string","field": "LLAVE_UNIDAD"},# 1-LLAVE_UNIDAD
        {"optional": True, "type": "string","field": "LLAVE_INCIDENTE"},# 2-LLAVE_INCIDENTE
        {"optional": True, "type": "string","field": "INCIDENTNUMBER"},# 3-NUMERO_INCIDENTE (Corregir)
        {"optional": True, "type": "string","field": "FECHA_CREACION"},# 4-FECHA_CREACION
        {"optional": True, "type": "string","field": "RECURSO"},# 5-RECURSO
        {"optional": True, "type": "int64","field": "PRIMERA_FECHA_UNIDAD"},# 6-PRIMERA_FECHA_UNIDAD
        {"optional": True, "type": "string","field": "ULTIMA_UBICACION"},# 7-ULTIMA_UBICACIÓN (Corregir)
        {"optional": True, "type": "int64","field": "TIEMPO_OPERACION"},# 8-TIEMPO_OPERACION
        {"optional": True, "type": "int64","field": "TIEMPO_DESPACHO"},# 9-TIEMPO_DESPACHO
        {"optional": True, "type": "int64","field": "TIEMPO_ENRUTA"},# 10-TIEMPO_ENRUTA
        {"optional": True, "type": "int64","field": "TIEMPO_ESCENA"},# 11-TIEMPO_ESCENA
        {"optional": True, "type": "int64","field": "PROPONIENDO"},# 12-PROPONIENDO
        {"optional": True, "type": "int64","field": "MOVILIZANDO"},# 13-MOVILIZANDO
        {"optional": True, "type": "int64","field": "ATENDIENDO"},# 14-ATENDIENDOFROM (Corregir)
        ],
        "name": "nuse_units_schema"
    }
    }
        
    p = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    try:
        # Ejecutar consulta en la base de datos
        # cursor.execute("SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_INCIDENTES;")
        # cursor.execute("SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_UNIDADES;")
        cursor.execute("SELECT * FROM datafeeds.sch_movilidad.View_INTERFAZ_MOVILIDAD_UNIDADES;")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        def corregir_noombres(x):
            if x=="NUMERO_INCIDENTE":
                r = 'INCIDENTNUMBER'
            # elif x=="ULTIMA_UBICACIÓN":
            #     r = 'ULTIMA_UBICACION'
            elif x=="ATENDIENDOFROM":
                r = 'ATENDIENDO'
            else:
                r=x
            return r
        columns = list(map(corregir_noombres, columns))
        string_serializer = StringSerializer('utf_8')
        for row in rows:
            val = [serialize_special_types(value) for value in row]
            payload = dict(zip(columns, val))
            pl = {'payload': payload}
            # Publicar cada fila en el tópico de Kafka
            p.send(topic,key = string_serializer(str(uuid4())), value=schema | pl)
        p.flush()
    except:
        print("Error de inserción en la base de datos o extracción de la base de datos P1")
    finally:
        cursor.close()
        cnxn.close()

## Tarea para Extraer y producir datos en topic CLONES
def task_produce_clones():
    topic = "nuse_mvcloned"
    cursor, cnxn = sql_server_connect()
    kafka_server = ["broker:29092"]
    schema = {"schema": {
        "type": "struct",
        "fields": [
        {"optional": True, "type": "string","field": "PADRE_INCIDENTE"},
        {"optional": True, "type": "int64","field": "FECHA_INCPADRE"},
        {"optional": True, "type": "string","field": "HIJO_UNIDAD"},
        {"optional": True, "type": "int64","field": "FECHA_INCHIJO"}
        ],
        "name": "nuse_cloned_schema"
    }
    }
        
    p = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    try:
        # Ejecutar consulta en la base de datos
        # cursor.execute("SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_INCIDENTES;")
        # cursor.execute("SELECT * FROM datafeeds.sch_movilidad.INTERFAZ_MOVILIDAD_CLONES;")
        cursor.execute("SELECT * FROM datafeeds.sch_movilidad.View_INTERFAZ_MOVILIDAD_CLONES;")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        def corregir_noombres(x):
            if x=="FECHA_PADRE":
                r = 'FECHA_INCPADRE'
            elif x=="FECHA_HIJO":
                r = 'FECHA_INCHIJO'
            else:
                r=x
            return r
        columns = list(map(corregir_noombres, columns))
        string_serializer = StringSerializer('utf_8')
        for row in rows:
            val = [serialize_special_types(value) for value in row]
            payload = dict(zip(columns, val))
            pl = {'payload': payload}
            # Publicar cada fila en el tópico de Kafka
            p.send(topic,key = string_serializer(str(uuid4())), value=schema | pl)
        p.flush()
    except:
        print("Error de inserción en la base de datos o extracción de la base de datos P1")
    finally:
        cursor.close()
        cnxn.close()


default_args = {
    'owner': 'Esteban Castro',
    'start_date': days_ago(0),
    'email': ['jcastrog@movilidadbogota.gov.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval':timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL-P1',
    default_args=default_args,
    description='ETL DAG using Bash',
    schedule_interval=timedelta(minutes=5),
)

# define the tasks

# Define primera tarea - Creación de topics y conectores si no existen
topic_connectors = PythonOperator(
    task_id='admin',
    python_callable=start_adm,
    dag=dag,
)
# Define segunda tarea - Extracción de datos de P1 hacia kafka - INCIDENTES
produce_incidents = PythonOperator(
    task_id='producer_incidents',
    python_callable=task_produce_inc,
    dag=dag,
)

# Define segunda tercera - Extracción de datos de P1 hacia kafka - UNIDADES
produce_units = PythonOperator(
    task_id='producer_units',
    python_callable=task_produce_units,
    dag=dag,
)

# Define segunda cuarta - Extracción de datos de P1 hacia kafka - CLONES
produce_clones = PythonOperator(
    task_id='producer_clones',
    python_callable=task_produce_clones,
    dag=dag,
)

# task pipeline
topic_connectors >> produce_incidents >> produce_units >> produce_clones