# de_proj_earthquake
pet project: earthquake data, Data Lake architecture

## Environment Setup

To create and activate the conda environment:

```bash
conda create -n de_proj_earthquake python=3.12
conda activate de_proj_earthquake
```

using default docker-compose.yaml

To download the docker-compose.yaml file:

```bash
curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml
```

metabase:
https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker


# для получения AIRFLOW UID создаем файл .env для текущего пользователя, и теперь мы можем получить доступ к папке с DAGs 
touch .env
echo "AIRFLOW_UID=$(id -u)" >> .env && echo "AIRFLOW_GID=$(id -g)" >> .env
cat .env

Разворачивание инфраструктуры:

docker-compose up -d


airflow доступен по: localhost:8080
airflow
airflow

minio доступен по: http://localhost:9001/
- MINIO_ROOT_USER=minioadmin
- MINIO_ROOT_PASSWORD=minioadmin

Создайте bucket:
prod
Создайте и сохраните ключ в cred.py (для безопасности добавлен в .gitignore)
