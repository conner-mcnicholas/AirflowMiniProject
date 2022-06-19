from datetime import date,datetime,timedelta
import yfinance as yf
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from os.path import expanduser

# Create the Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": date.today() - timedelta(days=1),
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)}

dag = DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval="0 6 * * 1-5")

# Create BashOperator to initialize tmp dir for data download
task0 = BashOperator(
    task_id = "t0",
    bash_command = "mkdir -p /tmp/data/{{ ds_nodash }}",
    dag = dag)

# Create PythonOperator to download the market data (t1,t2)
def download_data(sym):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    print("Downloading "+sym+"_data.csv to "+expanduser('~')+"/AirflowMiniProject/datadl")
    df = yf.download(sym,start=start_date,end=end_date,interval='1m')
    df.to_csv(expanduser('~')+"/AirflowMiniProject/datadl/"+sym+"_data.csv",header = True)
    print("Download complete!\n")

task1 = PythonOperator(
    task_id = "t1",
    python_callable = download_data,
    op_kwargs = {'sym': 'AAPL'},
    dag = dag)

task2 = PythonOperator(
    task_id = "t2",
    python_callable = download_data,
    op_kwargs = {'sym': 'TSLA'},
    dag = dag)

# Create BashOperator to move downloaded file to a data location
task3 = BashOperator(
    task_id = "t3",
    bash_command = "mv "+expanduser('~')+"/AirflowMiniProject/datadl/AAPL_data.csv /tmp/data/"+str(date.today()),
    dag = dag)

task4 = BashOperator(
    task_id = "t4",
    bash_command = "mv "+expanduser('~')+"/AirflowMiniProject/datadl/TSLA_data.csv /tmp/data/"+str(date.today()),
    dag = dag)

# Create PythonOperator to query data in both files in data location
def query_all():
    df_aapl = pd.read_csv("/tmp/data/"+str(date.today())+"/AAPL_data.csv")
    df_tsla = pd.read_csv("/tmp/data/"+str(date.today())+"/TSLA_data.csv")
    print('Executing query: "total_vol = df_aapl[\'Volume\']+df_tsla[\'Volume\']" ...')
    total_vol = df_aapl['Volume']+df_tsla['Volume']
    print("Total volume of shares traded across TSLA and AAPL on "+str(date.today())+": "+str(total_vol)+"\n")
    return total_vol

task5 = PythonOperator(
    task_id = "t5",
    python_callable = query_all,
    dag = dag)

chain(t0, [t1, t2], [t3, t4], t5)
