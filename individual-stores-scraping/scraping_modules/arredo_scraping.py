from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import requests
import logging
from google.cloud import bigquery


def arredo(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'ARREDO'

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name, datetime.now()))

    urls = [
        {'url': 'https://www.arredo.com.uy/habitacion/sabanas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/acolchados', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/frazadas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/mantas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/cubrecamas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/almohadas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/almohadones', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/cortinas', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/alfombras', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/cubresommier', 'scroll': 1},
        {'url': 'https://www.arredo.com.uy/habitacion/cubrecolchon', 'scroll': 1},
    ]

    html_dictionary = {}
    for URL in urls:

        if URL['scroll'] == 1:

            driver.get(URL['url'])
            time.sleep(2)
            scroll_pause_time = 1
            screen_height = driver.execute_script("return window.screen.height;")
            i = 1

            while True:

                driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
                i += 1
                time.sleep(scroll_pause_time)
                scroll_height = driver.execute_script("return document.body.scrollHeight;")
                if screen_height * i > scroll_height:
                    break
            time.sleep(2)
            html_dictionary['soup_{}'.format(URL['url'])] = BeautifulSoup(driver.page_source, 'html.parser')

        else:
            content = requests.get(URL['url'])
            if content.status_code == 200:
                html_dictionary['soup_{}'.format(URL['url'])] = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                break

        logging.info('       Finished Infinite Scroll for {} after {} scrolls at {}'.format(URL['url'], i, datetime.now()))

    driver.quit()
    logging.info('Webdriver closed after downloading HTML sourcecode for each page')

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for {} at {}'.format(store_name, datetime.now()))

    for category in html_dictionary:

        soup = html_dictionary[category]
        all_products = soup.find('div', {'class': 'product-list n1colunas'}).find_all('li', {
            'layout': '573175ce-660f-4cb4-b225-e883e175f240'})

        for product in all_products:
            product_name = product.find('span', {'class': 'product-description'}).get_text()
            product_link = product.find('a', {'class': 'product-link gtm-selector-LINK'})['href']

            product_price = float(
                product.find('span', {'class': 'best-price'}).get_text().replace('$', '').replace('.', ''))

            if product.find('span', {'class': 'old-price'}) is not None:
                product_price_lista = float(
                    product.find('span', {'class': 'old-price'}).get_text().replace('$', '').replace('.', ''))
            else:
                product_price_lista = product_price

            if '$' in product.find('span', {'class': 'best-price'}).get_text():
                price_currency = '$'
            else:
                price_currency = 'U$S'

            execution_date = datetime.utcnow()
            execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

            store.append(store_name)
            product_ID.append(product_name)
            price.append(product_price)
            price_lista.append(product_price_lista)
            currency.append(price_currency)
            url.append(product_link)
            timestamp.append(execution_date)
        logging.info('        Scraping completed for {} at {}'.format(category, datetime.now()))

    logging.info('      ***************************************************************************************** \n'
                 '                              Scraping completed for {} at {}                                   \n'
                 '                *****************************************************************************************'
                 .format(store_name, datetime.now()))


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
