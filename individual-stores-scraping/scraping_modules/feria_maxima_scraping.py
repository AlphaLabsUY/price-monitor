from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def feria_maxima(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Feria Maxima'

    categories = [
        {'url': 'https://www.feriamaxima.com/collections/indumentaria?page={}&sort_by=title-ascending',
         'pages': 3},
        {'url': 'https://www.feriamaxima.com/collections/zapatos?page={}&sort_by=title-ascending',
         'pages': 2},
        {'url': 'https://www.feriamaxima.com/collections/accesorios?page={}&sort_by=title-ascending',
         'pages': 6},
        {'url': 'https://www.feriamaxima.com/collections/petit?page={}&sort_by=title-ascending',
         'pages': 2},
        {'url': 'https://www.feriamaxima.com/collections/regaleria?page={}&sort_by=title-ascending',
         'pages': 1},
        {'url': 'https://www.feriamaxima.com/collections/decoracion?page={}&sort_by=title-ascending',
         'pages': 2}
    ]

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for {} at {}'.format(store_name, datetime.now()))

    for category in categories:
        for page in range(1, category['pages'] + 1):
            URL = category['url'].format(page)
            content = requests.get(URL)

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                break

            all_products = soup.find('div', {'class': 'grid grid--uniform grid--collection'}).find_all('div', {
                'class': 'grid-product__meta'})

            for product in all_products:

                product_name = product.find('div', {'class': 'grid-product__title grid-product__title--body'})\
                    .get_text()
                product_vendor = product.find('div', {'class': 'grid-product__vendor'}).get_text()
                product_name = product_name + ' ' + product_vendor

                product_link = None

                product_price = product.find('span', {'class': 'grid-product__price--original'})
                if product_price is not None:
                    product_price_lista = (product
                                           .find('div', {'class': 'grid-product__price'})
                                           .get_text().replace('Precio habitual', '')
                                           .replace('Precio de oferta', ' ')
                                           .replace('\n', ' ')
                                           .replace('.', '')
                                           .replace(',', '.')
                                           .replace('De', '')
                                           ).split()

                    product_price = product_price_lista[1]

                    if '$' in product_price:
                        price_currency = '$'
                    else:
                        price_currency = 'U$S'

                    product_price = float(product_price.replace('$', '').replace('U$S', ''))
                    product_price_lista = float(product_price_lista[0].replace('$', '').replace('U$S', ''))

                else:
                    product_price = (product
                                     .find('div', {'class': 'grid-product__price'})
                                     .get_text()
                                     .replace('De', '')
                                     .replace('.', '')
                                     .replace(',', '.')
                                     .replace('\n', ' ')
                                     .replace(' ', '')
                                     )
                    if '$' in product_price:
                        price_currency = '$'
                    else:
                        price_currency = 'U$S'

                    product_price = (product_price.replace('$', ' ').replace('U$S', ' ').split())
                    product_price = float(product_price[0])
                    product_price_lista = product_price

                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                product_ID.append(product_name)
                price.append(product_price)
                price_lista.append(product_price_lista)
                currency.append(price_currency)
                url.append(product_link)
                timestamp.append(execution_date)
            logging.info('        Scraping completed for category {} at {}'.format(URL, datetime.now()))
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
