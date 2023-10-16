import pandas as pd
from datetime import datetime,timedelta
import os
def created_date(df):
    df['created_date'] = datetime.now().strftime("%d/%m/%Y")

def calculate_due_date(row):
    if 'ngày' in row['due_time']:
        # Get the number of days from due_time
        days = int(row['due_time'].split()[1])
        # Convert current_date to datetime format
        current_date = datetime.strptime(row['created_date'], '%d/%m/%Y')
        # Calculate new date
        new_date = current_date + timedelta(days=days)
        return new_date.strftime("%d/%m/%Y")
    return row['current_date']

def transform_due_date(df):
    df['due_date'] = df['due_date'].apply(lambda x: x.split(':')[-1])
    # Apply calculate_due_date function to due_date column
    df.loc[df['due_date'] == '0', 'due_date'] = df[df['due_date'] == '0'].apply(calculate_due_date, axis=1)

def usd_to_vnd(salary_str):
    # Remove commas and letters from salary string
    salary_str = salary_str.replace(",", "").replace("USD", "").replace("Tới ", "").replace("Trên ", "")
    
    # Check the value and convert to integer
    if "-" in salary_str:
        salary = float(salary_str.split("-")[0])
    else:
        salary = float(salary_str)
    
    # Convert from USD to VND (assuming exchange rate of 1 USD = 23,000 VND)
    vnd_salary = salary * 23000 / 1000000
    
    return str(vnd_salary)

def vnd_to_vnd(salary_str):
    # Remove commas and letters from salary string
    salary_str = salary_str.replace(",", "").replace("triệu", "").replace("Tới ", "").replace("Trên ", "")
    
    # Check the value and convert to integer
    if "-" in salary_str:
        salary = float(salary_str.split("-")[0])
    else:
        salary = float(salary_str)
    
    return str(salary)

def transform_data():
    # read today data
    read_dir = '/home/truong/airflow/dags/Job_pipeline/Crawled_data'
    read_name = 'data_' + datetime.now().strftime("%d_%m_%Y") + '.csv'
    read_path = os.path.join(read_dir, read_name)
    df = pd.read_csv(read_path)

    created_date(df)
    transform_due_date(df)
    # Remove rows with salary = 'Thỏa thuận'
    df = df[df['salary'] != 'Thoả thuận']
    # Apply usd_to_vnd function to "salary" column
    df.loc[df['salary'].str.contains('USD'), 'salary'] = df[df['salary'].str.contains('USD')]['salary'].apply(usd_to_vnd)
    df.loc[df['salary'].str.contains('triệu'), 'salary'] = df[df['salary'].str.contains('triệu')]['salary'].apply(vnd_to_vnd)
    
    # transform due_date and created_date to datetime format
    df['due_date'] = df['due_date'].apply(lambda x: x.replace(" ",""))
    df['due_date'] = [datetime.strptime(date, '%d/%m/%Y').date() 
                            for date in df['due_date']]
    
    df['created_date'] = [datetime.strptime(date, '%d/%m/%Y').date() 
                            for date in df['created_date']]


    # write data after transform 
    write_dir = '/home/truong/airflow/dags/Job_pipeline/Transformed_data'
    write_name = 'data_' + 'transformed_' + datetime.now().strftime("%d_%m_%Y") + '.csv'
    write_path = os.path.join(write_dir, write_name)
    df.to_csv(write_path, index=False)