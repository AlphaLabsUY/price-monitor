from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def dimm(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'DIMM'

    categories = [
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.186&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2463&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2481&ps=128&order=1&mo={}',
     'pages': [1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2450&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.567&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2605&ps=128&order=1&mo={}',
     'pages': [1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2395&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1, 2]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.814&ps=128&order=1&mo={}',
     'pages': [1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2147&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2623&ps=128&order=1&mo={}',
     'pages': [1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2386&ps=128&order=1&mo=1&pagina={}',
     'pages': [0, 1]},
    {'url': 'https://www.dimm.com.uy/productos/productos.php?secc=productos&path=0.2539&ps=128&order=1&mo={}',
     'pages': [1]},
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
        for page in category['pages']:
            URL = category['url'].format(page)
            content = requests.get(URL)

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                break

            all_products = soup.find_all('article', {'itemtype': 'http://schema.org/Product'})

            for product in all_products:

                product_price_lista = product.find('div', {'class': 'precio_antes'})

                if product_price_lista is not None:
                    product_price_lista = float(product_price_lista.find('span', {'class': 'antes'}).get_text())
                    product_price = float(
                        product.find('div', {'class': "precio_cont"}).find('span', {'itemprop': 'price'}).get_text())
                    price_currency = product.find('span', {'class': 'pmoneda'}).get_text().replace('$U', '$').replace(
                        'USD', 'U$S')
                else:
                    product_price_lista = product.find('span', {'itemprop': 'price'})

                    if product_price_lista is not None:
                        product_price_lista = float(product_price_lista.get_text())
                        product_price = product_price_lista
                        price_currency = product.find('span', {'class': 'pmoneda'}).get_text().replace('$U',
                                                                                                       '$').replace(
                            'USD', 'U$S')
                    else:
                        product_price_lista = float(0)
                        product_price = float(0)
                        price_currency = ''

                product_name = product.find('span', {'itemprop': 'name'})

                if product_name is not None:
                    product_name = product_name.get_text()
                else:
                    product_name = 'No name given'.format(page, URL)
                    print('No name in page {} in URL {}'.format(page, URL))

                product_link = 'https://www.dimm.com.uy/' + product.find('div', {'class': 'accont'}).find('a')['href']

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

    logging.info('***************************************************************************************** \n'
                 '                                          Scraping completed for {} at {}                                   \n'
                 '                        *****************************************************************************************'
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