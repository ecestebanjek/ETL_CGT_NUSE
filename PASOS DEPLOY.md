# PASOS PARA DESPLIEGUE DEL ETL EN EL SERVIDOR
1. git clone XXXXX
2. docker compose build
3. docker compose up airflow-init
4. docker compose up -d
5. docker exec -it connect bash
6. cd /usr/share/confluent-hub-components
7. curl https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz | tar -xz
8. /bin/confluent-hub install confluentinc/kafka-connect-jdbc:10.7.0 --no-prompt
9. exit
10. docker restart connect

## Verificación de funcionamiento
http://localhost:9021 --> Verificación de broker, topics, conectores e interfaz

http://localhost:8080 --> Verificación de ETL de extracción y producción funcional