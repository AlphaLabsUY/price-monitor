from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def rapsodia(table_id, csv_path=None, webdriver_path=None):

    store_name = 'Rapsodia'

    categories = [
                 {"url": "https://rapsodia.com.uy/rapsodia-1/woman.html?p={}", "pages": 8},
                 {"url": "https://rapsodia.com.uy/rapsodia-1/home-collection.html?p={}", "pages": 1},
                 {"url": "https://rapsodia.com.uy/rapsodia-1/vintage.html?p={}", "pages": 10},
                 # {"url": "https://rapsodia.com.uy/rapsodia-1/uni.html?p={}", "pages": 2}
                 ]

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for {}'.format(store_name))

    for category in categories:
        for page in range(1, category["pages"] + 1):
            URL = category['url'].format(page)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                              ' AppleWebKit/537.36 (KHTML, like Gecko)'
                              ' Chrome/85.0.4183.102'
                              ' Safari/537.36'}

            content = requests.get(URL, headers=headers)

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                break

            all_products = soup.find('div', {'class': 'grid-3-4-desktop grid-2-mobile'}).find_all('div', {'class': 'datos'})

            for product in all_products:

                product_name = product.find('h3', {'class': 'nombre'}).get_text()
                product_link = product['href']

                if product.find('p', {'class': 'old-price'}) is not None:
                    product_price_lista = (
                        product.find('p', {'class': 'old-price'}).find('span', {'class': 'price'}).get_text())
                    product_price = (
                        product.find('p', {'class': 'special-price'}).find('span', {'itemprop': 'price'}).get_text()
                        )

                else:
                    product_price = (product.find('span', {'itemprop': 'price'}).get_text()
                                     )
                    product_price_lista = product_price

                if '$U' in product_price:
                    price_currency = '$'
                else:
                    price_currency = 'U$S'

                product_price = float(product_price.replace('$U\xa0', '').replace('.', ''))
                product_price_lista = float(product_price_lista.replace('$U\xa0', '').replace('.', ''))

                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                timestamp.append(execution_date)
                url.append(product_link)
                price.append(product_price)
                price_lista.append(product_price_lista)
                product_ID.append(product_name)
                currency.append(price_currency)
            logging.info('        Scraping completed for category {}'.format(URL.format(page)))

    logging.info('***************************************************************************************** \n'
                 '                                                      Scraping completed for {}              '
                 '                    \n'
                 '                                 *************************************************'
                 '****************************************'
                 .format(store_name))

    logging.info('Pandas DataFrame to be created')
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

    logging.info('Pandas DataFrame correctly created')

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

    logging.info('Finished to load Data into BigQuery')

    date = datetime.now()
    date = date.strftime("%m%d%Y")
    df_final.to_csv('C:/Users/USUARIO/OneDrive/19_ALPHALABS/1_PROYECTOS/5_CEDU_Price_Monitor/'
                    '3_REPOSITORY/individual-stores-scraping/Downloaded_CSVs/{}_{}.csv'.format(store_name, date),
                    encoding='utf-8-sig')

    logging.info('Finished to load Data into CSV at {}'.format(datetime.now()))


