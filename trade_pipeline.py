import logging
from datetime import date,datetime,timedelta
import yfinance as yf
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models.baseoperator import chain
import pandas as pd

today = str(date.today())

# Create the Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": timezone.pendulum.today() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)}

dag = DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval="0 18 * * 1-5")

# Create BashOperator to initialize tmp dir for data download
t0 = BashOperator(
    task_id = "task0",
    bash_command = f"mkdir -p /tmp/data/{today}",
    dag = dag)

# Create PythonOperator to download the market data (t1,t2)
def download_data(sym):
    #dynamic config solution for manually triggered weekend tests.
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(sym,start=start_date,end=end_date,interval='1m')
    while len(df) == 0:
        start_date = start_date-timedelta(days=1)
        end_date = start_date+timedelta(days=1)
        df = yf.download(sym,start=start_date,end=end_date,interval='1m')
    print(f"    SUCCESS -> Downloaded {len(df)} rows  of {sym} data for {str(start_date)} \n")
    df.to_csv(f"{sym}_data.csv", header = True)

t1 = PythonOperator(
    task_id = "task1",
    python_callable = download_data,
    op_kwargs = {'sym': 'AAPL'},
    dag = dag)

t2 = PythonOperator(
    task_id = "task2",
    python_callable = download_data,
    op_kwargs = {'sym': 'TSLA'},
    dag = dag)

# Create BashOperator to move downloaded file to a data location
t3 = BashOperator(
    task_id = "task3",
    bash_command = f"mv /opt/airflow/AAPL_data.csv /tmp/data/{today}",
    dag = dag)

t4 = BashOperator(
    task_id = "task4",
    bash_command = f"mv /opt/airflow/TSLA_data.csv /tmp/data/{today}",
    dag = dag)

# Create PythonOperator to query data in both files in data location
def query_all():
    tmpdir = f"/tmp/data/{today}/"
    df_aapl = pd.read_csv(f"{tmpdir}/AAPL_data.csv")
    df_tsla = pd.read_csv(f"{tmpdir}/TSLA_data.csv")

    print('Executing query: \
    "total_vol = pd.DataFrame(df_aapl.Volume).join( \
    df_tsla.Volume.rename(\'Vol2\')).sum(axis=0)"')

    total_vol = pd.DataFrame(df_aapl.Volume).join(df_tsla.Volume.rename('Vol2')).sum(axis=0)
    total_vol_str = "Total AAPL Shares Traded: "+'{:,}'.format(total_vol[0])+" , Total TSLA Shares Traded: "+'{:,}'.format(total_vol[1])+'\n'
    print(f"RESULTS -> {total_vol_str}")
    with open(f"{tmpdir}/daily_trade_volumes.txt", "w") as txtfile:
        txtfile.write(total_vol_str)

t5 = PythonOperator(
    task_id = "task5",
    python_callable = query_all,
    dag = dag)

chain(t0, [t1, t2], [t3, t4], t5)
