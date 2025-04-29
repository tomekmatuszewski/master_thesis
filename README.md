Master thesis

Analysis of the use of the Apache Iceberg format in the construction of ETL/ELT flows and the Lakehouse architecture on the example of NBA league statistical data

Prerequisites:

    - Docker installed on local machine
    - Docker compose installed om local machine


To run -> 'docker compose up' from terminal
it takes a while to pull all images

Components are available under the hosts listed below:


Airflow -> http://localhost:5050 (to login username 'airflow', password 'airflow')

![alt text](readme_images/airflow.png)

![alt text](readme_images/airflow2.png)

MinIO -> http://localhost:9001 (login username 'admin', password 'password') 

![alt text](readme_images/minio.png)

Spark -> http://localhost:8080

![alt text](readme_images/spark.png)

Jupyter Notebook -> http://localhost:8888

![alt text](readme_images/jupyter.png)

SuperSet -> http://localhost:8088/superset/dashboard/1/ (path to dashboards)
(username 'admin', password 'admin')

![alt text](readme_images/superset1.png)

![alt text](readme_images/superset2.png)

to load dashboard go to Dashboards tab then arrow in the right top corner - 'import dashboards'
select zip file dashboard.zip from superset directory

![alt text](readme_images/superset3.png)
