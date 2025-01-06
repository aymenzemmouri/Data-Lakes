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
    # Votre code ici ...
)

first_step = BashOperator(
    # Votre code ici ...
)

# Bonne chance !