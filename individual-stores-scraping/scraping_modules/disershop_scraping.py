from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def disershop(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Disershop'

    categories = [
        # {'url': 'https://disershop.com.uy/vestimenta-todos?sort=p.sort_order&order=ASC&page={}', 'pages': 7},
        # {'url': 'https://disershop.com.uy/emprendedores?sort=pd.name&order=ASC&limit=100&page={}', 'pages': 2},
        {'url': 'https://disershop.com.uy/oficina-y-hogar?sort=pd.name&order=ASC&limit={}00', 'pages': 1},
        # {'url': 'https://disershop.com.uy/papel?sort=pd.name&order=ASC&limit={}00', 'pages': 1},
        # {'url': 'https://disershop.com.uy/packaging?sort=pd.name&order=ASC&limit={}00', 'pages': 1},
        # {'url': 'https://disershop.com.uy/estampados?sort=pd.name&order=ASC&limit={}00', 'pages': 1},
        # {'url': 'https://disershop.com.uy/sublimacion?sort=pd.name&order=ASC&limit=100&page={}', 'pages': 2},
        # {'url': 'https://disershop.com.uy/electrodomesticos?sort=pd.name&order=ASC&limit={}00', 'pages': 1},
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

            all_products = soup.find_all('div', {'class': 'col-sm-25 col-xs-6'})

            for product in all_products:
                product_link = product.find('div', {'class': 'image'}).find('a')['href']

                product_price_lista = product.find('span', {'class': 'price-old'})
                if product_price_lista is not None:
                    product_price_lista = (product_price_lista
                                           .get_text()
                                           .replace('\n', '')
                                           .replace('\n\t\t\t\t\t\t\n\t\t', '')
                                           .replace('.', '')
                                           .replace(',', '.')
                                           )

                    product_price = (product.find('span', {'class': 'price-new'})
                                     .get_text()
                                     .replace('\n', '')
                                     .replace('\n\t\t\t\t\t\t\n\t\t', '')
                                     .replace('.', '')
                                     .replace(',', '.')
                                     )

                else:
                    product_price = (product.find('div', {'class': 'price'})
                                     .get_text()
                                     .replace('\n', '')
                                     .replace('\n\t\t\t\t\t\t\n\t\t', '')
                                     .replace('.', '')
                                     .replace(',', '.')
                                     )

                    product_price_lista = product_price

                if '$' in product_price:
                    price_currency = '$'
                else:
                    price_currency = 'U$S'

                product_price = float(product_price.replace('$', '').replace('US', ''))
                product_price_lista = float(product_price_lista.replace('$', '').replace('US', ''))

                product_name = product.find('div', {'class': 'name'}).get_text()

                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                product_ID.append(product_name)
                price.append(product_price)
                price_lista.append(product_price_lista)
                currency.append(price_currency)
                url.append(product_link)
                timestamp.append(execution_date)
                print(product_price, product_name, product_link)
            logging.info('        Scraping completed for category {} at {}'.format(URL, datetime.now()))

    logging.info('      ***************************************************************************************** \n'
                 '                              Scraping completed for {} at {}                                   \n'
                 '               *****************************************************************************************'
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
