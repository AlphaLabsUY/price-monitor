from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import logging
from google.cloud import bigquery


def canva(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Canva Store'

    logging.info('Scraping set to Start for {}'.format(store_name))

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name))

    categories = [
        {'url': 'https://www.canvastore.com.uy/mochilas-2#/pageSize=20&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 1},
        {'url': 'https://www.canvastore.com.uy/accesorios#/pageSize=20&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 4},
        {'url': 'https://www.canvastore.com.uy/carteras-de-dama#/pageSize=20&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 3},
        {'url': 'https://www.canvastore.com.uy/vestimenta-dama#/pageSize=9&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 4},
        {'url': 'https://www.canvastore.com.uy/vestimenta#/pageSize=20&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 5},
        {'url': 'https://www.canvastore.com.uy/new-arrivals#/pageSize=9&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 2},
        {'url': 'https://www.canvastore.com.uy/ropa-beb%C3%A9#/pageSize=9&viewMode=grid&orderBy=5&pageNumber={}',
         'pages': 4},
                ]

    html_dictionary = {}

    for category in categories:
        for page in range(1, category['pages'] + 1):

            driver.get(category['url'].format(page))

            time.sleep(3)
            html_dictionary['soup_{}'.format(category['url'].format(page))] = \
                BeautifulSoup(driver.page_source, 'html.parser')

    driver.quit()

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

        if soup.find('div', {'class': 'page-body'}) is not None:
            all_products = soup.find('div', {'class': 'page-body'}).find_all('div', {'class': 'item-box'})
        else:
            logging.error('-> Error to load Page {}\n'
                          '                            '.format(category))
            continue

        for product in all_products:
            product_price = product.find('span', {'class': 'price actual-price'}).get_text()

            if '$' in product_price:
                price_currency = '$'
            else:
                price_currency = 'U$S'

            product_price = float(product_price.replace('$', '').replace('.', ''))

            if product.find('span', {'class': 'price old-price'}) is not None:
                product_price_lista = float(product.find('span', {'class': 'price old-price'})
                                            .get_text()
                                            .replace('$', '')
                                            .replace('.', ''))
            else:
                product_price_lista = product_price

            product_name = product.find('div', {'class': 'details'}).find('h2', {'class': 'product-title'}).find(
                'a').get_text()

            product_link = ('https://www.canvastore.com.uy' + product.find('div', {'class': 'details'})
                                                                      .find('h2', {'class': 'product-title'})
                                                                      .find('a')['href'])

            execution_date = datetime.utcnow()
            execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

            store.append('Canva Store')
            product_ID.append(product_name)
            price.append(product_price)
            price_lista.append(product_price_lista)
            currency.append(price_currency)
            url.append(product_link)
            timestamp.append(execution_date)
        logging.info('        Scraping completed for category {}'
                     .format(category))
    logging.info('                     Scraping completed for {} at {}'.format(store_name, datetime.now()))

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
