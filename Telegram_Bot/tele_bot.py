from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackContext
import Response as R
import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read_file(open('/home/truong/airflow/dags/Job_pipeline/config.cfg')) # sua path
HOST = config.get('POSTGRES', 'HOST')
DB_NAME = config.get('POSTGRES', 'DB_NAME')
DB_USER = config.get('POSTGRES', 'DB_USER')
DB_PASSWORD = config.get('POSTGRES', 'DB_PASSWORD')
API_TOKEN = config.get('TELEGRAM', 'API_TOKEN')


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f'Hello {update.effective_user.first_name}!, Welcome to the bot')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('How can I help you?')

# Responses to messages
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = str(update.message.text).lower()
    response = R.sample_response(text)
    await update.message.reply_text(response)

def execute_sql_query(sql_query):
    connect_params = {
        "host" : HOST,
        "dbname" : DB_NAME,
        "user" : DB_USER,
        "password" : DB_PASSWORD
    }
    conn = psycopg2.connect(**connect_params)
    cur = conn.cursor()
    conn.set_session(autocommit= True)

    cur.execute(sql_query)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

async def get_job_description(update: Update, context: CallbackContext):
    try:
        min_salary = context.args[0]
        max_salary = context.args[1]
        jobs_number = context.args[2]
        sql_query = f"""
            SELECT job_title, salary || ' triệu' ,due_time, link_jd FROM job_description
            WHERE salary between '{min_salary}' and '{max_salary}'
            AND work_location like '%Hà Nội%'
            ORDER BY due_date, salary DESC
            limit '{jobs_number}'
            """

        results = execute_sql_query(sql_query)
        for result in results: 
            await update.message.reply_text(result[0] + "\n" + result[1] + "\n" + result[2] + "\n" + result[3])
    except:
        await update.message.reply_text("Please enter the correct format: /job_description min_salary max_salary jobs_number")

def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Update {update} cause error {context.error}")

if __name__ == '__main__':
    print("Bot is running...")
    app = Application.builder().token(API_TOKEN).build()
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('job_description', get_job_description))

    app.add_handler(MessageHandler(filters.TEXT,handle_message))

    app.add_error_handler(error)

    # Polls the bot
    print("Bot is polling...")
    app.run_polling(poll_interval=3)
