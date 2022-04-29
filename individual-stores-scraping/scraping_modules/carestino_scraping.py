from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery
from utils import get_html


def carestino(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Carestino'
    URL = 'https://www.carestino.com.uy/productos/'
    content = get_html(URL)

    if content.reason == 'Not Found':
        logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
        raise KeyError('Page Not Found')

    soup = BeautifulSoup(content.text, 'html.parser')

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for {} at {}'.format(store_name, datetime.now()))

    all_products = soup.find_all('div', {'class': "info-product col-xs-12 col-xl-12 boxProductos"})

    for product in all_products:

        exectution_date = datetime.utcnow()
        execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

        product_link = product.find('a', {'class': "not-href-hover"})['href']
        product_name = product.find('a', {'class': "not-href-hover"}).get_text()
        price_currency = product.find('span', {'class': 'price_symbol'}).get_text()

        if price_currency == '$':
            price_currency = '$'
        else:
            price_currency = 'U$S'

        product_price = float(
            product.find('span', {'class': 'price_fraction'}).get_text().replace('.00', '').replace('.', '').replace(',',
                                                                                                                     '.'))
        product_price_lista = product.find('div', {'class': 'price_old'})

        if product_price_lista is not None:
            product_price_lista = float(
                product_price_lista.get_text().replace('.00', '').replace('.', '').replace(',', '.').replace('$', ''))
        else:
            product_price_lista = product_price

        store.append(store_name)
        product_ID.append(product_name)
        price.append(product_price)
        price_lista.append(product_price_lista)
        currency.append(price_currency)
        url.append(product_link)
        timestamp.append(execution_date)
        print(product_name, product_link, product_price)

    logging.info('                Scraping completed for {} at {}'.format(store_name, datetime.now()))

    df_final = pd.DataFrame({

        "store": store,
        "product": product_ID,
        "price": price,
        "price_no_discount": price_lista,
        "price_currency": currency,
        "installments": None,
        "installment_payment": float(0),
        "installment_currency": '',
        "url": url,
        "timestamp": timestamp})

    df_final["timestamp"] = pd.to_datetime(df_final["timestamp"])

    len_prev_drop = len(df_final)
    df_final = df_final.drop_duplicates(subset=['product', 'price', 'price_no_discount'], keep='first')
    dropped_items = len(df_final) - len_prev_drop
    logging.warning('Dropped {} duplicated Items'.format(dropped_items))

    client = bigquery.Client()
    logging.info('Initiated BigQuery Client: {}'.format(client))

    table_id = table_id

    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("store", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("price_no_discount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("price_currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("installments", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("installment_payment", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("installment_currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
    ])

    job = client.load_table_from_dataframe(df_final,
                                           table_id,
                                           job_config=job_config)
    job.result()

    date = datetime.now()
    date = date.strftime("%m%d%Y")
    df_final.to_csv('C:/Users/USUARIO/OneDrive/19_ALPHALABS/1_PROYECTOS/5_CEDU_Price_Monitor/'
                    '3_REPOSITORY/individual-stores-scraping/Downloaded_CSVs/{}_{}.csv'.format(store_name, date),
                    encoding='utf-8-sig')

    logging.info('Finished to load Data into BigQuery at {}'.format(datetime.now()))
