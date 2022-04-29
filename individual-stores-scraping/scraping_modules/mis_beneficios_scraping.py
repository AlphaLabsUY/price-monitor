from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def mis_beneficios(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Mis Beneficios'

    categories = [
        {'url': 'https://www.misbeneficios.com.uy/aniversario-misbe?p={}&product_list_order=name', 'pages': 4},
        {'url': 'https://www.misbeneficios.com.uy/free-shipping?p={}&product_list_order=name', 'pages': 2},
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/quedate-en-casa/id/2223/?p={}&product_list_order=name','pages': 3 },
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/nuestros-elegidos/id/1717/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/tv-y-audio/id/9310/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/grandes-electros/id/9131/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/peque-os-electros/id/9272/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/cuidado-personal/id/9261/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/tecnologia?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/hogar?p={}&product_list_order=name', 'pages': 2},
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/bebes/id/2384/?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/colchones-y-sommiers?p={}&product_list_order=name','pages': 1 },
        {'url': 'https://www.misbeneficios.com.uy/apple?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/samsung?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/lg?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/sony?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/bosch?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/philips/id/1199/?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/irobot/id/1406/?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/dolce-gusto?p={}&product_list_order=name', 'pages': 1},
        {'url': 'https://www.misbeneficios.com.uy/catalog/category/view/s/whirlpool/id/2291/?p={}&product_list_order=name', 'pages': 1 },
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
                break

            all_products = soup.find_all('li', {'class': 'item product product-item'})
            for product in all_products:
                product_name = product.find('h2',
                                            {'class': 'product name product-item-name misbe-title'})\
                    .get_text().replace('\n', '')

                product_link = product.find('h2', {'class': 'product name product-item-name misbe-title'}).find('a')['href']

                product_price_lista = product\
                    .find('span', {'class': 'price-container price-final_price tax'}).find('span', {'class': 'price'})\
                    .get_text()

                product_price_lista = float(product_price_lista
                                            .replace('U$S', '')
                                            .replace('$', '')
                                            .replace('.', '')
                                            .replace(',', '.'))

                product_price = float(product.find('span', {'class': 'price-wrapper'})
                                      .get_text()
                                      .replace('U$S', '')
                                      .replace('$', '')
                                      .replace('.', '')
                                      .replace(',', '.'))

                currency_data = product.find('span', {'class': 'price-wrapper'}).get_text()

                if 'U$S' in currency_data:
                    price_currency = 'U$S'
                else:
                    price_currency = '$'

                exectution_date = datetime.utcnow()
                execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

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
