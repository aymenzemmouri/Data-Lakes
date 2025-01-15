from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# Ici, vous pouvez définir votre DAG en utilisant l'exemple que vous ai fournis du TP4. 
# N'oubliez pas de schedule votre DAG pour qu'il se trigger à un intervale régulier, disons 30mn
dag = DAG(    
    "hackernews_pipeline",
    default_args=default_args,
    description='Pipeline ETL pour le traitement des données de hackernews',
    schedule=timedelta(days=1),
)

extract_from_hn_api = BashOperator(
    task_id='hn_api',
    bash_command='python /opt/airflow/build/hn_api.py --limit 50 --endpoint-url http://localhost:4566',
    dag=dag,
)

# Store in elasticsearch
store_elasticseach = BashOperator(
    task_id='es_handler',
    bash_command='python /opt/airflow/scripts/es_handler.py es_handler.py --host localhost --port 9200 --index hackernews --endpoint-url http://localhost:4566',
    dag=dag,
)


# Définir l'ordre des tâches
extract_from_hn_api >> store_elasticseach