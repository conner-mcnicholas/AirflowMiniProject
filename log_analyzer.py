from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, date
from airflow.utils.dates import days_ago
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'loganalyzer',
    default_args=default_args,
    description='Log Analyzer',
    schedule_interval='0 19 * * 1-5',
    start_date=days_ago(1)
)

def analyze_file(**kwargs):
    logfiles = Path('/opt/airflow/logs/marketvol/{}'.format(kwargs['file'])).rglob('*.log')
    error_count = 0
    error_lines = []
    for file in logfiles:
        with open(file, 'r') as logfile:
            for line in logfile:
                if 'ERROR' in line:
                    error_count += 1
                    error_lines.append(line)
    return error_count, error_lines

t1 = PythonOperator(
    task_id='createdir',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task0'},
    dag=dag
)

t2 = PythonOperator(
    task_id='appledata',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task1'},
    dag=dag
)

t3 = PythonOperator(
    task_id='tesladata',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task2'},
    dag=dag
)

t4 = PythonOperator(
    task_id='moveapple',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task3'},
    dag=dag
)

t5 = PythonOperator(
    task_id='movetesla',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task4'},
    dag=dag
)

t6 = PythonOperator(
    task_id='querydata',
    python_callable=analyze_file,
    provide_context=True,
    op_kwargs={'file': 'task5'},
    dag=dag
)

def print_results(**kwargs):
    ti = kwargs['ti']
    t1_count, t1_lines = ti.xcom_pull(task_ids='createdir')
    t2_count, t2_lines = ti.xcom_pull(task_ids='appledata')
    t3_count, t3_lines = ti.xcom_pull(task_ids='tesladata')
    t4_count, t4_lines = ti.xcom_pull(task_ids='moveapple')
    t5_count, t5_lines = ti.xcom_pull(task_ids='movetesla')
    t6_count, t6_lines = ti.xcom_pull(task_ids='querydata')
    error_count = t1_count + t2_count + t3_count + t4_count + t5_count + t6_count
    error_lines = t1_lines + t2_lines + t3_lines + t4_lines + t5_lines + t6_lines
    print('ERROR COUNT is {}'.format(error_count))
    for line in error_lines:
        print(line)

t7 = PythonOperator(
    task_id='print_results',
    python_callable=print_results,
    provide_context=True,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
