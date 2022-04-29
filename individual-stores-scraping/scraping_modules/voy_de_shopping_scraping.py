from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import logging
from google.cloud import bigquery


def voydeshopping(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Voy de Shopping'

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name, datetime.now()))

    urls = [
        {'url': 'https://www.voydeshopping.uy/moda-mujer/?sort=alphaasc&page={}', 'pages': 141,
         "product_field": "product has-custom-displays"},
        {'url': 'https://www.voydeshopping.uy/moda-hombre/?sort=alphaasc&page={}', 'pages': 42, "product_field": "product"},
        {'url': 'https://www.voydeshopping.uy/bebidas/?sort=alphaasc&page={}', 'pages': 47, "product_field": "product"},
        {'url': 'https://www.voydeshopping.uy/tecnologia/?sort=alphaasc&page={}', 'pages': 138, "product_field": "product"},
    ]

    html_dictionary = {}
    for URL in urls:
        for page in range(1, URL['pages']+1):
            url = URL['url'].format(page)
            driver.get(url)
            time.sleep(3.5)
            html_dictionary['soup_{}'.format(url)] = BeautifulSoup(driver.page_source, 'html.parser')

    driver.quit()

    logging.info('       Webdriver closed after downloading HTML sourcecode for each page')

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

        try:
            check_loading = soup.find('h1', {'class': 'page-heading'}).get_text()
        except Exception as e:
            logging.error('Error loading:', category)
            logging.error(e)
            continue

        if check_loading != 'Error 404 - No se encontró la página':

            if 'moda-mujer' in category:
                product_field = "product has-custom-displays"
            else:
                product_field = "product"

            all_products = soup.find('ul', {'class': 'productGrid'}).find_all('li', {'class': '{}'.format(product_field)})

            for product in all_products:

                product_name = product.find('h4', {'class': 'card-title'}).get_text().replace('\n', '')
                product_link = product.find('h4', {'class': 'card-title'}).find('a')['href']

                product_price = (product.find('div', {'class': 'container-price price-card'})
                                 .get_text()
                                 .replace('\n\n$U', '')
                                 .replace('\n', '')
                                 .replace('.', '')
                                 .replace(',', '.')
                                 .replace('USD', '')
                                 .replace('$U', '')
                                 .split())
                if len(product_price) == 2:
                    product_price_lista = float(product_price[0])
                    product_price = float(product_price[1])
                elif len(product_price) == 3:
                    product_price_lista = float(product_price[2])
                    product_price = float(product_price[2])

                else:
                    product_price = float(product_price[0])
                    product_price_lista = product_price

                if '$U' in product.find('div', {'class': 'container-price price-card'}).get_text():
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
            logging.info('        Scraping completed for category {} at {}'.format(category, datetime.now()))
        else:
            logging.error(soup.find('h1', {'class': 'page-heading'}).get_text())

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