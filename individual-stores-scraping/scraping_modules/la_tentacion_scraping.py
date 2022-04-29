from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery
import sys


def la_tentacion(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'La Tentacion'

    categories = [
                  "https://latentacion.com.uy/categoria-producto/electronica-audio-y-video/page/{}/?orderby=price-desc",
                  # 'https://latentacion.com.uy/categoria-producto/hogar-muebles-y-jardin/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/dormitorio/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/colchones-y-sommiers/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/celulares-y-telefonia/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/computacion/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/deportes-y-fitness/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/herramientas-y-construccion/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/salud-y-belleza/page/{}/?orderby=price-desc',
                  # 'http://latentacion.com.uy/categoria-producto/pinturas/page/{}/?orderby=price-desc',
                  # 'https://latentacion.com.uy/categoria-producto/motos/page/{}/?orderby=price-desc'
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
        for page in range(0, sys.maxsize):
            URL = category.format(page)
            content = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                break

            all_products_content = soup.find('ul',
                                             {'class': 'products products-loop row grid product-infinite_scroll products-loop-column-4 ciyashop-products-shortcode mobile-col-2'})
            all_products = all_products_content.find_all('li')

            for product in all_products:
                product_price_lista = product.find('span', {'class': 'price'}).find('del')
                if product_price_lista is not None:
                    product_price_lista = product_price_lista.get_text()
                    product_price = product.find('span', {'class': 'price'}).find('ins').get_text()

                    if 'USD' in product_price:
                        price_currency = 'U$S'
                    else:
                        price_currency = '$'

                    product_price = float(product_price.replace('USD', '').replace('$', '').replace('.', ''))
                    product_price_lista = float(product_price_lista.replace('USD', '').replace('$', '').replace('.', ''))

                else:
                    product_price = product.find('span', {'class': 'woocommerce-Price-amount amount'}).get_text()

                    if 'USD' in product_price:
                        price_currency = 'U$S'
                    else:
                        price_currency = '$'

                    product_price = float(product_price.replace('USD', '').replace('$', '').replace('.', ''))
                    product_price_lista = product_price

                product_name = product.find('h3', {'class': 'product-name'}).find('a').get_text().replace('\n\t\t\t','').replace('\t\t','')
                product_link = product.find('h3', {'class': 'product-name'}).find('a')['href']

                exectution_date = datetime.utcnow()
                execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                product_ID.append(product_name)
                price.append(product_price)
                price_lista.append(product_price_lista)
                currency.append(price_currency)
                url.append(product_link)
                timestamp.append(execution_date)
                print(product_name, product_price, product_link)
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
    dropped_items = len_prev_drop - len(df_final)
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