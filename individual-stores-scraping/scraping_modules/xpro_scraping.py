from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def xpro(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'XPRO'

    categories = [{"category": 'Audio-Pro', "limit": 100, "pages": 4},
                  {"category": 'home-audio', "limit": 100, "pages": 1},
                  {"category": 'audio-comercial', "limit": 100, "pages": 1},
                  {"category": 'dj', "limit": 100, "pages": 1},
                  {"category": 'recording', "limit":100, "pages": 2},
                  {"category": 'cases', "limit": 100, "pages": 2},
                  {"category": 'truss-stage', "limit": 100, "pages": 1},
                  {"category": 'iluminacion', "limit": 100, "pages": 1},
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

        for page in range(1, category["pages"] + 1):

            URL = 'https://uy.xprostore.com/{}?limit={}&page={}'.format(category["category"],
                                                                        category["limit"],
                                                                        page)
            content = requests.get(URL)

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                continue

            products_all = soup.find_all('div', {'class': 'product-layout has-extra-button'})

            for product in products_all:

                product_name = product.find('div', {'class': 'name'}).get_text()
                product_link = product.find('div', {'class': 'name'}).find('a')['href']

                if product.find('span', {'class': 'price-new'}) is not None:
                    product_price = product.find('span', {'class': 'price-new'}).get_text()
                    product_price_lista = product.find('span', {'class': 'price-old'}).get_text()

                else:
                    product_price = product.find('span', {'class': 'price-normal'}).get_text()
                    product_price_lista = product_price

                if 'u$s' in product_price:
                    product_currency = 'U$S'
                    product_price = float(product_price.replace(',', '').replace('u$s', ''))
                    product_price_lista = float(product_price_lista.replace(',', '').replace('u$s', ''))
                else:
                    product_price = float(0)
                    product_price_lista = float(0)
                    product_currency = 'NA'

                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                timestamp.append(execution_date)
                url.append(product_link)
                price.append(product_price)
                store.append(store_name)
                product_ID.append(product_name)
                currency.append(product_currency)
                price_lista.append(product_price_lista)
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
    logging.warning('Dropped {} duplicated Items from {}'.format(dropped_items, len_prev_drop))

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

