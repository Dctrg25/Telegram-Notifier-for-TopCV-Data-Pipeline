import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import time 
from datetime import datetime
import undetected_chromedriver as uc
from selenium import webdriver
import os
options = webdriver.ChromeOptions() 
options.binary_location = '/home/truong/chrome-linux64/chrome'

def get_url():
    driver = uc.Chrome(browser_executable_path='/home/truong/chrome-linux64/chrome')
    driver.get('https://www.topcv.vn/viec-lam-it?sort=up_top&skill_id=&skill_id_other=&keyword=&company_field=&position=&salary=&page=1')
    return driver

def get_job_title(output, driver):
    job_title = driver.find_elements(By.XPATH,'//*[@id="main"]/div[1]/div[3]/div[4]/div[1]/div[1]/div[*]/div/div[2]/div[2]/h3')
    for i in job_title:
        output['job_title'].append(i.text)

def get_company_name(output, driver):
    company_name = driver.find_elements(By.XPATH,'//*[@id="main"]/div[1]/div[3]/div[4]/div[1]/div[1]/div[*]/div/div[2]/a')
    for i in company_name:
        output['company_name'].append(i.text)

def get_due_time(output, driver):
    due_time = driver.find_elements(By.CLASS_NAME,'time')
    for i in due_time:
        output['due_time'].append(i.text)

def get_link_jd(output, driver):
    link_jds = driver.find_elements(By.XPATH, '//*[@id="main"]/div[1]/div[3]/div[4]/div[1]/div[1]/div[*]/div/div[2]/div[2]/h3/a')
    link_jd = [i.get_attribute('href') for i in link_jds]
    return link_jd

def get_jd(output,link):
    output['link_jd'].append(link)
    driver = uc.Chrome(browser_executable_path='/home/truong/chrome-linux64/chrome')
    driver.get(link)
    time.sleep(5)
    click_ = driver.find_element(By.XPATH, '//*[@id="modal-brand-communication"]/div/div/div[1]/button')
    click_.click()

    time.sleep(5)

    try:
        job_description = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/div[1]/div/div[3]/div/div[2]/div[3]/div[1]')
        benefit = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/div[1]/div/div[3]/div/div[2]/div[3]/div[3]')
        qualification = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/div[1]/div/div[3]/div/div[2]/div[3]/div[2]')
        work_location = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/div[1]/div/div[3]/div/div[2]/div[2]/div')
        due_date = "0"
        salary = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div/div[1]/div/div[3]/div/div[2]/div[1]/div/div[1]/div[2]/span')

    except:
        job_description = driver.find_element(By.XPATH, '//*[@id="box-job-information-detail"]/div[1]/div/div[1]/div')
        benefit = driver.find_element(By.XPATH, '//*[@id="box-job-information-detail"]/div[1]/div/div[3]/div')
        qualification = driver.find_element(By.XPATH, '//*[@id="box-job-information-detail"]/div[1]/div/div[2]/div')
        work_location = driver.find_element(By.XPATH, '//*[@id="box-job-information-detail"]/div[1]/div/div[4]/div')
        due_date = driver.find_element(By.XPATH, '//*[@id="box-job-information-detail"]/div[2]/div[3]')
        salary = driver.find_element(By.XPATH, '//*[@id="header-job-info"]/div/div[1]/div[2]/div[2]')

    output['salary'].append(salary.text)
    output['job_description'].append(job_description.text)
    output['qualification'].append(qualification.text)
    output['benefit'].append(benefit.text)
    output['work_location'].append(work_location.text)
    try:
        output['due_date'].append(due_date.text)
    except:
        output['due_date'].append(due_date)
    driver.quit()

def extract():
    output = {
    'job_title' : [],
    'company_name' : [],
    'due_time' : [],
    'salary' : [],
    'job_description' : [] ,
    'qualification' : [],
    'benefit' : [],
    'work_location' : [],
    'due_date' : [],
    'link_jd' : []
}
    driver = get_url()
    time.sleep(5)
    page = 3
    while(1):
        get_job_title(output, driver)
        get_company_name(output, driver)
        get_due_time(output, driver)
        link_jd = get_link_jd(output, driver)
        for i in link_jd:
            try:
                get_jd(output,i)
            except:
                continue
        click_ = driver.find_element(By.XPATH, '//*[@id="main"]/div[1]/div[3]/div[4]/div[1]/div[2]/nav/ul/li['+str(page)+']')
        time.sleep(5)
        click_.click()
        page += 1
        if page == 6:
            break
    driver.quit()
    df = pd.DataFrame(output)
    write_dir = '/home/truong/airflow/dags/Job_pipeline/Crawled_data'
    file_name = 'data_' + datetime.now().strftime("%d_%m_%Y") + '.csv'
    file_path = os.path.join(write_dir, file_name)
    df.to_csv(file_path, index=False)
    print('Done')