from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# import libraries 
from pandas import pandas as pd
from bs4 import BeautifulSoup
import requests, smtplib, pytz

def extract_naiin_data():
    titles = []
    writers = []
    publishers = []
    full_prices = []
    net_prices = []
    item_votes = []

    # for i in range(1,2):
    for i in range(1,15):

        # Connect to Website and pull in data

        URL = f'https://www.naiin.com/category?category_1_code=13&product_type_id=3&categoryLv2Code=139&sortBy=bestseller&pageNo={i}'
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/5377.36", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"closee", "Upgrade-Insecure-Requests":"1"}

        page = requests.get(URL, headers=headers)

        soup1 = BeautifulSoup(page.content, "html.parser")
        soup2 = BeautifulSoup(soup1.prettify(), "html.parser")

        all_book_data = soup2.find_all('div', class_='productitem mt5 mb35 catitem catitem-grid item mb30')

        for link in all_book_data:

            # Extract the title tag
            title_tag = link.find('a', class_='itemname')
            title = title_tag.get_text().strip().replace(',', '') if title_tag else None
            titles.append(title)

            # Extract the writer tag
            writer_tag = link.find('a', class_='txt-light txt-custom-light')
            writer = writer_tag.get_text().strip() if writer_tag else '-'
            writers.append(writer)

            # Extract the publisher tag
            publisher_tag = link.find('a', class_='cate-publisher txt-light')
            publisher = publisher_tag.get_text().strip() if publisher_tag else '-'
            publishers.append(publisher)

            # Extract the sale-price
            full_price_tag = link.find('p', class_='sale-price')
            full_price = full_price_tag.get_text().strip().split()[0] if full_price_tag else '0.0'
            full_prices.append(full_price)

            # Extract the net-price
            net_price_tag = link.find('p', class_='txt-price')
            net_price = net_price_tag.get_text().strip().split()[0] if net_price_tag else '0.0'
            net_prices.append(net_price)

            # Extract the item-vote
            itme_vote_tag = link.find('span', class_='item-vote')
            item_vote = itme_vote_tag.get_text().strip().split()[0] if itme_vote_tag else '0'
            item_votes.append(item_vote)

    result = {'titles' : titles, 'writers' : writers, 'publishers' : publishers, 'full_prices' : full_prices, 'net_prices' : net_prices, 'item_vottes' : item_votes}
    return result

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.tsk_extract_ebook_naiin_data")

    # Create a DataFrame
    data = {'title': data['titles'], 'writer' : data['writers'], 'publisher' : data['publishers'], 'full_price': data['full_prices'], 'net_price' : data['net_prices'], 'item_vote' : data['item_votes'], 'platform' : 'NAIIN'}
    df_naiin = pd.DataFrame(data)

    # df_naiin['Net-price'] = 0 if df_naiin['Net-price'] == 'Free' else df_naiin['Net-price']
    df_naiin['net_price'] = pd.to_numeric(df_naiin['net_price'].replace('Free', 0).replace(',', ''), errors='coerce').fillna(0)
    df_naiin['full_price'] = df_naiin['full_price'].str.replace(',', '').astype(float)
    df_naiin['item_vote'] = df_naiin['item_vote'].str.replace(',', '').astype(int)

    # df_naiin.insert(0, 'Datetime', pd.to_datetime('now'))

    df_naiin.to_csv("ebook_naiin_data.csv", index=False, header=False)

def load_naiin_data():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY ebook_details_naiin FROM stdin WITH DELIMITER as ','",
        filename='ebook_naiin_data.csv'
    )

def save_joined_data_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="task_join_data")
    df = pd.DataFrame(data, columns=['title', 'writer', 'net_price', 'platform_n', 'value', 'platform_m'])
    df['difference'] = round(abs(df['net_price'] - df['value']), 2)
    df['cheapest_platform'] = df.apply(lambda row: row['platform_n'] if row['net_price'] < row['value'] else row['platform_m'], axis=1)
    df.rename(columns={'net_price': 'price(naiin)', 'value': 'price(meb)'}, inplace=True)
    df = df[['title', 'price(naiin)', 'price(meb)', 'difference', 'cheapest_platform']]
    # df.to_csv("joined_weather_data.csv", index=False)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_ebook_data_' + dt_string
    df.to_csv(f"s3://testing-airflow-ymlo/output/{dt_string}.csv", index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag_2',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
            task_id = 'tsk_start_pipeline'
        )
                        
        join_data = PostgresOperator(
                task_id='task_join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    COALESCE(n.title, m.title) AS title,                    
                    n.writer,
                    n.net_price,
                    n.platform as platform_n,
                    m.value,
                    m.platform as platform_m
                    FROM ebook_details_naiin n
                    INNER JOIN e_book_meb m
                        ON n.title = m.title                                      
                ;
                '''
            )

        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_s3
            )

        end_pipeline = DummyOperator(
                task_id = 'task_end_pipeline'
        )

        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_naiin_api") as group_A:
            create_table_1 = PostgresOperator(
                task_id='tsk_create_table_1',
                postgres_conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS e_book_meb (
                    title TEXT NOT NULL,
                    writer TEXT NULL,
                    value FLOAT NOT NULL,
                    rating NUMERIC NULL,
                    platform TEXT NOT NULL
                    );

                '''
            )

            truncate_table = PostgresOperator(
                task_id='tsk_truncate_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE e_book_meb;
                    '''
            )

            truncate_naiin_table = PostgresOperator(
                task_id='tsk_truncate_naiin_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE ebook_details_naiin;
                    '''
            )

            uploadS3_to_postgres  = PostgresOperator(
                task_id = "tsk_uploadS3_to_postgres",
                postgres_conn_id = "postgres_conn",
                sql = "SELECT aws_s3.table_import_from_s3('e_book_meb', '', '(format csv, DELIMITER '','', HEADER true)', 'testing-airflow-ymlo',  'book_details_meb.csv', 'ap-southeast-1');"
            )
            
            create_table_2 = PostgresOperator(
                task_id='tsk_create_table_2',
                postgres_conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS ebook_details_naiin (
                    title TEXT NOT NULL,
                    writer TEXT NULL,
                    publisher TEXT NULL,
                    full_price float NOT NULL,
                    net_price float NOT NULL,
                    item_vote numeric NULL,
                    platform TEXT NOT NULL  
                );
                '''
            )

            # is_ebook_naiin_api_ready = HttpSensor(
            #     task_id ='tsk_is_naiin_api_ready',
            #     http_conn_id='ebook_naiin_api',
            #     endpoint='/category?category_1_code=13&product_type_id=3&categoryLv2Code=139&sortBy=bestseller&pageNo=1'
            # )

            extract_ebook_naiin_data = PythonOperator(
                task_id= 'tsk_extract_ebook_naiin_data',
                python_callable=extract_naiin_data
            )

            transform_load_ebook_naiin_data = PythonOperator(
                task_id= 'tsk_transform_load_naiin_data',
                python_callable=transform_load_data
            )

            load_ebook_naiin_data = PythonOperator(
            task_id= 'tsk_load_naiin_data',
            python_callable=load_naiin_data
            )

            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> truncate_naiin_table >> extract_ebook_naiin_data >> transform_load_ebook_naiin_data >> load_ebook_naiin_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline