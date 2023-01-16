# Тестовое задание Spark 

**Description**: Test ELT data-platform 


**Step 1 (Docker start)**:  
sudo service docker start  
sudo service docker status  

**Step 2 (Docker Compose)**:  
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
docker network inspect platform_default  
- set connection with ip "Gateway": "172.23.0.1" (example)  
- Connection Id: clickhouse_default  
- Host: 172.23.0.1 (example)  
- Port: 9000  


TODO:  
