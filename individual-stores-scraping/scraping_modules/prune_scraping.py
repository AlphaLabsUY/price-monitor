from utils import export_data_to_bigquery_csv
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import logging
from google.cloud import bigquery


def prune(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'PRUNE'

    categories = [
                 {"url": "https://www.prune.com.uy/carteras/ver-todo.html?p={}", "pages": 13},
                 {"url": "https://www.prune.com.uy/zapatos/ver-todo.html?p={}", "pages": 3},
                 {"url": "https://www.prune.com.uy/accesorios/ver-todo.html?p={}", "pages": 4},
                 {"url": "https://www.prune.com.uy/camperas.html?p={}", "pages": 1},
                 {"url": "https://www.prune.com.uy/sale-aw-20/carteras.html", "pages": 1},
                 {"url": "https://www.prune.com.uy/sale-aw-20/zapatos/ver-todos.html", "pages": 1},
                 {"url": "https://www.prune.com.uy/sale-aw-20/camperas.html", "pages": 1},
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
            URL = category["url"]
            content = requests.get(URL.format(page))

            if content.status_code == 200:
                soup = BeautifulSoup(content.text, 'html.parser')
            else:
                logging.error('Error for store {} with status code {}'.format(store_name, content.status_code))
                break

            if 'ver-todo' in URL:
                all_products = soup.find("ol", {"class": "products list items product-items"}).find_all("li")
            elif 'sale' in URL:
                all_products = soup.find("ol", {"class": "products list items product-items"}).find_all("li")
            else:
                all_products = soup.find("ol", {"class": "products list items product-items special"}).find_all("li")

            for product in all_products:
                product_name = product.find("a", {"class": "product-item-link"}).get_text()
                product_name = (product_name.replace('\r\n', '')).replace(' ', '')

                product_link = product.find("a")["href"]

                if product.find('span', {'data-price-type': 'oldPrice'}) is not None:
                    # product_price_lista = float(product.find('span', {'data-price-type': 'oldPrice'})
                    #                             .find('span', {'class': 'price'}).get_text()
                    #                             .replace('$', '')
                    #                             .replace('USD', '')
                    #                             .replace('.', '')
                    #                             .replace(',', ''))
                    product_price = float(product.find('span', {'data-price-type': 'finalPrice'})
                                          .find('span', {'class': 'price'}).get_text()
                                          .replace('$', '')
                                          .replace('USD', '')
                                          .replace('.', '')
                                          .replace(',', ''))

                    product_price_lista = product_price

                    if "$" in product.find('span', {'data-price-type': 'finalPrice'}).find('span',
                                                                                           {'class': 'price'}).get_text():

                        currency_product = "$"
                    else:
                        currency_product = "U$S"

                else:
                    product_price = float(product.find("span", {"class": "price"}).get_text()
                                          .replace('$', '')
                                          .replace('USD', '')
                                          .replace('.', '')
                                          .replace(',', ''))
                    product_price_lista = product_price

                    if "$" in product.find("span", {"class": "price"}).get_text():
                        currency_product = "$"
                    else:
                        currency_product = "U$S"
                print(product_name, product_price, product_price_lista)
                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                timestamp.append(execution_date)
                url.append(product_link)
                price.append(product_price)
                price_lista.append(product_price_lista)
                product_ID.append(product_name)
                currency.append(currency_product)
                print(product_name, product_price, product_link)
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
    logging.warning('Dropped {} duplicated Items from {}'.format(dropped_items, len_prev_drop))

    logging.info('Pandas DataFrame correctly created')

    export_data_to_bigquery_csv(df=df_final,
                                table_id=table_id,
                                store_name=store_name,
                                csv_path=csv_path,
                                )


