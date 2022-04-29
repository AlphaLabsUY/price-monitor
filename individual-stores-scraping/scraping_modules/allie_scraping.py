from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def allie(table_id, csv_path=None, webdriver_path=None):

    store_name = 'Allie'

    categories = [
                 {"url": "https://allie.com.uy/vestimenta.html?p={}", "pages": 26},
                 {"url": "https://allie.com.uy/calzado.html?p={}", "pages": 3},
                 {"url": "https://allie.com.uy/accesorios.html?p={}", "pages": 7},
                 # {"url": "https://allie.com.uy/sale.html?p={}", "pages": 24}
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
            URL = category["url"].format(page)
            content = requests.get(URL)

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(URL, content.status_code))
                continue

            all_products = soup.find("ol", {"class": "products list items product-items grid-2"}).find_all("li")
            for product in all_products:

                product_name = product.find("a", {"class": "product-item-link"}).get_text()
                product_name = (product_name
                                .replace('\r\n', '')
                                .replace('\n', '')
                                .replace('  ', '')
                                )

                product_link = product.find("a", {"class": "product-item-link"})['href']

                if product.find('span', {'data-price-type': 'oldPrice'}) is not None:
                    product_price_lista = float(product.find('span', {'data-price-type': 'oldPrice'})
                                           .find("span", {"class": "price"})
                                           .get_text()
                                           .replace('UYU', '')
                                           .replace('UYU', '')
                                           .replace('.', '')
                                           )
                    product_price = float(product.find('span', {'data-price-type': 'finalPrice'})
                                     .find("span", {"class": "price"})
                                     .get_text()
                                     .replace('UYU', '')
                                     .replace('UYU', '')
                                     .replace('.', '')
                                     )
                else:
                    product_price = float(product.find('span', {'data-price-type': 'finalPrice'})
                                     .find("span", {"class": "price"})
                                     .get_text()
                                     .replace('UYU', '')
                                     .replace('UYU', '')
                                     .replace('.', '')
                                     )
                    product_price_lista = product_price

                if "UYU" in (product.find('span', {'data-price-type': 'finalPrice'})
                                    .find("span", {"class": "price"})
                                    .get_text()):
                    price_currency = "$"
                else:
                    price_currency = "U$S"

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
    dropped_items = len(df_final) - len_prev_drop
    logging.warning('Dropped {} duplicated Items'.format(dropped_items))

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
