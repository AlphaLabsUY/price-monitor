from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import logging
logging.getLogger('Price Monitor').setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s : %(message)s')


def sodimac(table_id, csv_path=None, webdriver_path=None):

    store_ = 'SODIMAC'

    timestamp = []
    URL = []
    price = []
    price_lista = []
    store_name = []
    product_ID = []
    currency = []
    sub_category_ID = []

    one_pager = []

    URL_home = 'https://www.sodimac.com.uy/sodimac-uy/category/cat1140002/Camping'
    content = requests.get(URL_home)

    if content.status_code == 200:
        soup = BeautifulSoup(content.text, 'html.parser')

        all_cats = soup.find_all('div', {'class': 'jsx-717690720 flyout-container'})
        for cat in all_cats:

            category_name = cat.find('div', {'class': "jsx-717690720 flyout-header"}).get_text()
            all_sub_cats = cat.find_all('li', {'class': 'jsx-5832954 service-item'})
            for sub_cat in all_sub_cats:
                sub_cat_name = sub_cat.find('a').get_text()
                sub_cat_link = sub_cat.find('a')['href']

                URL_sub_category = 'https://www.sodimac.com.uy' + sub_cat_link

                content = requests.get(URL_sub_category)

                if content.status_code == 200:
                    soup = BeautifulSoup(content.text, 'html.parser')
                else:
                    logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                    break

                max_pages_all = soup.find_all('button', {'class': "jsx-4278284191"})

                if len(max_pages_all) != 0:
                    max_pages = [p.get_text() for p in max_pages_all]
                    max_pages = max([int(p) for p in list(filter(None, max_pages))])

                    for page in range(1, max_pages + 1):
                        page_ = '?currentpage={}&sortBy=variant.name%2Casc'.format(page)
                        URL_page = URL_sub_category + page_

                        headers = {
                            "accept-encoding": "gzip, deflate, br",
                            "accept-language": "es-ES,es;q=0.9,en;q=0.8",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
                        }

                        attempts = 0
                        while attempts < 3:
                            try:
                                content = requests.get(URL_page, headers)
                                break
                            except Exception as e:
                                logging.error(e)
                                attempts += 1

                        if content.status_code == 200:
                            soup = BeautifulSoup(content.text, 'html.parser')
                        else:
                            logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                            break

                        products_all = soup.find_all('div', {'itemprop': 'offers'})

                        for product in products_all:

                            product_link = 'https://www.sodimac.com.uy' + product.find('a')['href']
                            product_name = product.find('h2').get_text()

                            product_price_box = product.find('div', {"class": "jsx-585964327 main gridView"})
                            if product_price_box is not None:
                                product_price = product_price_box.find('span', {'class': "jsx-4135487716"}).get_text().replace(
                                    '.', '')
                            else:
                                product_price_box = product.find('div', {"class": "jsx-585964327 main gridView AB"})
                                product_price = product_price_box.find('span', {'class': "jsx-4135487716"}).get_text().replace(
                                    '.', '')

                            if '$' in product_price:
                                product_currency = '$'
                                product_price = float(product_price.replace('$', ''))
                            elif 'USD ' in product_price:
                                product_currency = 'U$S'
                                product_price = float(product_price.replace('USD ', ''))
                            else:
                                product_price = float(0)
                                logging.error('Error for store {}'.format(store_name))

                            product_price_lista_box = product.find('div', {"class": "jsx-585964327 sub gridView"})
                            if product_price_lista_box is not None:
                                product_price_lista = product_price_lista_box.find_all('span', {'class': "jsx-4135487716"})
                                product_price_lista = [p.get_text() for p in product_price_lista][2]\
                                    .replace('.', '')\
                                    .replace('$', '')\
                                    .replace('USD', '')
                                product_price_lista = float(product_price_lista)
                            else:
                                product_price_lista_box = product.find('div', {"class": "jsx-585964327 sub gridView AB"})
                                if product_price_lista_box is not None:
                                    product_price_lista = product_price_lista_box.find_all('span', {'class': "jsx-4135487716"})
                                    product_price_lista = [p.get_text() for p in product_price_lista][2]\
                                        .replace('.', '')\
                                        .replace('$', '')\
                                        .replace('USD', '')
                                    product_price_lista = float(product_price_lista)
                                else:
                                    product_price_lista = product_price

                            execution_date = datetime.utcnow()
                            execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                            timestamp.append(execution_date)
                            URL.append(product_link)
                            price.append(product_price)
                            store_name.append(store_)
                            product_ID.append(product_name)
                            currency.append(product_currency)
                            price_lista.append(product_price_lista)
                            sub_category_ID.append(sub_cat_name)
                        logging.info('        Scraping completed for category {}'.format(URL_page))

                else:
                    one_pager.append(URL_sub_category)
                    logging.info('{} has only one page'.format(URL_sub_category))

        logging.info('***************************************************************************************** \n'
                     '                                                      Scraping completed for {}              '
                     '                    \n'
                     '                                 *************************************************'
                     '****************************************'
                     .format(store_))

        logging.info('Pandas DataFrame to be created')
        df_final = pd.DataFrame({

            "store": store_name,
            "product": product_ID,
            "price": price,
            "price_no_discount": price_lista,
            "price_currency": currency,
            "installments": None,
            "installment_payment": float(0),
            "installment_currency": '',
            "url": URL,
            "timestamp": timestamp})

        df_final["timestamp"] = pd.to_datetime(df_final["timestamp"])

        len_prev_drop = len(df_final)
        df_final = df_final.drop_duplicates(subset=['product', 'price', 'price_no_discount'], keep='first')
        dropped_items = len(df_final) - len_prev_drop
        logging.warning('Dropped {} duplicated Items from {}'.format(dropped_items, len_prev_drop))

        logging.info('Pandas DataFrame correctly created')

    else:
        logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))

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

    logging.info('Finished to load Data into BigQuery at {}'.format(datetime.now()))

    date = datetime.now()
    date = date.strftime("%m%d%Y")
    df_final.to_csv('C:/Users/USUARIO/OneDrive/19_ALPHALABS/1_PROYECTOS/5_CEDU_Price_Monitor/'
                    '3_REPOSITORY/individual-stores-scraping/Downloaded_CSVs/{}_{}.csv'.format(store_, date),
                    encoding='utf-8-sig')

    logging.info('Finished to load Data into CSV at {}'.format(datetime.now()))
