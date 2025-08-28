import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ##########################
# generate account dimention data , save into csv
#############################

start_date = datetime(2024, 9, 15)
defaultargs = {
    'owner': 'x1nguoguo',
    'depends_on_past': False,
    'backfill': False
}

# pre-define args:
# num_rows: num of rows generate_account_dim_data function will generated
num_rows = 50
output_file = './account_dim_large_data.csv'

# staging generated data
account_ids = []
account_types = []
statuses = []
customer_ids = []
balances = []
opening_dates = []


def generate_random_data(row_num:int):
    '''
    args:
    row_num: row number, int

    generate single record of account dimention table
    six fields: account_id, account_type, status, 
                customer_id, balance, and open date
    '''
    account_id = f'A{row_num:08d}'
    account_type = random.choice(['SAVINGS', "CHECKING"])
    status = random.choice(['ACTIVE', 'INACTIVE'])
    customer_id = f'C{random.randint(1, 1000):05d}'
    balance = round(random.uniform(100.00, 10000.00), 2)

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)

    return account_id, account_type, status, customer_id, balance, opening_date_millis

def generate_account_dim_data():
    '''
    generate pre-defined number of rows, and form a dataframe
    '''
    row_num = 1
    while row_num <= num_rows:
        account_id, account_type, status, customer_id, balance, opening_date_millis = generate_random_data(row_num)
        account_ids.append(account_id)
        account_types.append(account_type)
        statuses.append(status)
        customer_ids.append(customer_id)
        balances.append(balance)
        opening_dates.append(opening_date_millis)
        row_num += 1

    df = pd.DataFrame({
        'account_id': account_ids,
        'account_type': account_types,
        'status': statuses,
        'customer_id': customer_ids,
        'balance': balances,
        'opening_date': opening_dates
    })

    df.to_csv(output_file, index=False)

    print(f'CSV file {output_file} with {num_rows} rows has been generated successfully!')


with DAG('account_dim_generator',
    default_args=defaultargs,
    description='Generate large account dimension data in a CSV file',
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=['schema'])as dag:

    start = EmptyOperator(
        task_id='start_task'
    )

    generate_account_dimension_data = PythonOperator(
        task_id='generate_account_dim_data',
        python_callable=generate_account_dim_data
    )

    end = EmptyOperator(
        task_id='end_task'
    )

    start >> generate_account_dimension_data >> end