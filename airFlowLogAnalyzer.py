from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from pathlib import Path
from datetime import datetime, timedelta

LOG_PATH = '/Users/machome/airflow/logs'

default_args = {
    'owner': 'admin',
    'start_date': datetime(2021,3,5)

}

# Initialize the DAG
dag = DAG('analyzeLog',
	      default_args=default_args,
	      description='A simple DAG on stock market volume',
	      schedule_interval="@daily"
	      )

def analyzeLogFiles(**kwargs):
    
    log_path = kwargs['log_path']
    symbol = kwargs['symbol']

    file_list = Path(log_path).rglob('*.log')

    error_list = []
    filelist = list(file_list)

    for file in filelist:
        file_str = str(file)
        if file_str.find("marketvol") != -1 and file_str.find(symbol) != -1:
            log_file = open(file_str, 'r')
            for line in log_file:
                if "ERROR" in line:
                    error_list.append(line)

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='error_count', value=len(error_list))
    task_instance.xcom_push(key='errors', value=error_list)

# Parse log files for TSLA
task1 = PythonOperator(dag=dag,
                       task_id='tsla_log_errors',
                       python_callable=analyzeLogFiles,
                       provide_context=True,
                       op_kwargs={'symbol': 'tsla','log_path':LOG_PATH})

templated_command1 = """
  echo -e "total error count:" {{ task_instance.xcom_pull(task_ids='tsla_log_errors', key='error_count') }} "\nList of errors for TESLA:" {{ task_instance.xcom_pull(task_ids='tsla_log_errors', key='errors') }} 
"""

# Print log errors for TSLA error log files
task2 = BashOperator(task_id='print_log_error_tsla',
                     bash_command=templated_command1,
                     dag=dag)

# Parse log files for AAPL
task3 = PythonOperator(dag=dag,
                       task_id='aapl_log_errors',
                       python_callable=analyzeLogFiles,
                       provide_context=True,
                       op_kwargs={'symbol': 'aapl','log_path':LOG_PATH})

templated_command2 = """
  echo -e "total error count:" {{ task_instance.xcom_pull(task_ids='aapl_log_errors', key='error_count') }} "\nList of errors for APPLE:" {{ task_instance.xcom_pull(task_ids='aapl_log_errors', key='errors') }} 
"""

# Print log errors for AAPL error log files
task4 = BashOperator(task_id='print_log_error_aapl',
                     bash_command=templated_command2,
                     dag=dag)

task1 >> task2
task3 >> task4