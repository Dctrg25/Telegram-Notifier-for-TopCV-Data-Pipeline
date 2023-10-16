# Create procedure to daily update due_time column

import psycopg2
import configparser

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/Job_pipeline/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')
# Connect to Postgres
connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
conn = psycopg2.connect(**connect_params)

# Create a cursor object
cur = conn.cursor()
conn.set_session(autocommit= True)
# Create the job_description table with output1 column
cur.execute("""
    CREATE OR REPLACE FUNCTION update_due_time() RETURNS void AS $$
    DECLARE
        today_date date;
    BEGIN
        -- Lấy ngày hôm nay
        SELECT current_date INTO today_date;

        -- Tính toán số ngày khác biệt giữa due_date và ngày hôm nay
        UPDATE job_description
        SET due_time = 'còn ' || (due_date - today_date) || ' ngày để ứng tuyển';
    END;
    $$ LANGUAGE plpgsql;
""")

# Close the cursor and connection
cur.close()
conn.close()

