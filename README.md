# Тестовое задание Airflow 

**Description**: Test ELT data-platform with Airflow 2.5.1 and ClickHouse server version 22.1.3 and client version 22.1.3.7  
API https://exchangerate.host/#/


**Step 1 (Docker start)**:  
```sh
sudo service docker start
```  
```sh
sudo service docker status 
```  


**Step 2 (Docker Compose)**:  
```sh
cd <path-to-docker-compose.yaml>  
```  
```sh
mkdir ./dags ./logs ./plugins ./sql_requests  
```  
```sh
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .e
```  
```sh
sudo docker-compose up airflow-init  
```  
```sh
sudo docker-compose up -d 
```  


**Step 3 (Airflow UI)**:  
 http://localhost:8080/
- login: airflow  
- password: airflow  


**Step 4 (ClickHouse)**:  
ClickHouse exposes 8123 port for HTTP interface and 9000 port for native client.  
```sh
sudo docker-compose exec click_server clickhouse-client  
```  
```sh
exit  
```


**Step 5 (Connection)**:  
```sh
docker network ls  
```
```sh
docker network inspect ddl  
```  
Get ip from "Gateway": "172.50.0.1" (example)  

Set Connections:

| Key             | Value              |
|-----------------|--------------------|
| Connection Id   | clickhouse_default |
| Connection Type | Sqlite             |
| Host            | 172.50.0.1         |
| Login           | default            |
| Port            | 9000               |


### TODO:    
- (DONE) Historical case  (API example url = 'https://api.exchangerate.host/timeseries?start_date=2022-01-01&end_date=2022-01-09&symbols=BTC,USD')  
- (DONE) Develop template_searchpath for sql script with ClickHouseOperator  
- Small data - XCom, Big Data - Custom XCom Backends  
- Discuss datamart format and partitioning  
- Make changes to sql scripts as needed  
