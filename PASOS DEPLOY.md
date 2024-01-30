# PASOS PARA DESPLIEGUE DEL ETL EN EL SERVIDOR

1. git clone [XXXXX](https://github.com/ecestebanjek/ETL_CGT_NUSE.git)
2. echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env ## En caso de instalación en servidor linux
3. docker compose build
4. docker compose up airflow-init
5. docker compose up -d
6. docker exec -it connect bash
7. cd /usr/share/confluent-hub-components
8. curl https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz | tar -xz
9. /bin/confluent-hub install confluentinc/kafka-connect-jdbc:10.7.0 --no-prompt
10. exit
11. docker restart connect

Espera de 3-5 minutos para que todo cargue, y verificar:

## Verificación de funcionamiento
http://localhost:9021 --> Verificación de broker, topics, conectores e interfaz

http://localhost:8080 --> Verificación de ETL de extracción y producción funcional