# Тестовое задание Airflow 

**Description**: Test ELT data-platform with Airflow 2.4.1 and ClickHouse server version 22.1.3 and client version 22.1.3.7


**Step 1 (Docker start)**:  
sudo service docker start  
sudo service docker status  


**Step 2 (Docker Compose)**:  
cd <path-to-docker-compose.yaml>  
mkdir ./dags ./logs ./plugins ./sql_requests  
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env  
sudo docker-compose up airflow-init  
sudo docker-compose up -d  


**Step 3 (Airflow UI)**:  
http://localhost:8080/  
- login: airflow  
- password: airflow  


**Step 4 (ClickHouse)**:  
- ClickHouse exposes 8123 port for HTTP interface and 9000 port for native client.  
sudo docker-compose exec click_server clickhouse-client  
exit  


**Step 5 (Connection)**:  
docker network ls  
docker network inspect data-platform_default  
- set connection ip from "Gateway": "172.23.0.1" (example)  
- Connection Id: clickhouse_default  
- Host: 172.23.0.1 (example)  
- Login: default  
- Port: 9000  


TODO:  
- Historical case  (API example url = 'https://api.exchangerate.host/timeseries?start_date=2022-01-01&end_date=2022-01-09&symbols=BTC,USD')  
- Develop template_searchpath for sql script with ClickHouseOperator  
- Small data - XCom, Big Data - Custom XCom Backends  
- Discuss datamart format and partitioning  
- Make changes to sql scripts as needed  
