from utils import get_html
from utils import export_data_to_bigquery_csv
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import logging


def woow(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'WOOW'

    categories = [
                 {"url": 'https://shop.woow.com.uy/electrodomesticos?p={}&product_list_order=name',
                  "pages": 5},
                 {"url": 'https://shop.woow.com.uy/televisores?p={}&product_list_order=name',
                  "pages": 1},
                 {"url": 'https://shop.woow.com.uy/notebooks-tecnologia?p={}&product_list_order=name',
                  "pages": 3},
                 {"url": 'https://shop.woow.com.uy/muebles?p={}&product_list_order=name',
                  "pages": 4},
                 {"url": 'https://shop.woow.com.uy/hogar?p={}&product_list_order=name',
                  "pages": 4},
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
        for page in range(1, category['pages'] + 1):
            URL = category['url'].format(page)

            try:
                content = get_html(URL)
            except Exception as e:
                logging.info('Not possible to load {} after 4 attempts'.format(URL))
                logging.info(e)
                continue

            soup = BeautifulSoup(content.text, 'html.parser')

            if soup.find('ol', {'class': 'products list items product-items'}) is not None:
                all_products = (soup.find('ol', {'class': 'products list items product-items'})
                                .find_all('li', {'class': 'item product product-item'}))
            else:
                logging.error('Found empty HTML, skiped to next page')
                continue

            for product in all_products:
                product_name = product.find('a', {'class': 'product-item-link'}).get_text().replace('\n', '')
                product_link = product.find('a', {'class': 'product-item-link'})['href']

                if product.find('span', {'class': 'special-price'}) is not None:
                    product_price_lista = (product.find('span', {'class': 'old-price'})
                                           .find('span', {'class': 'price'})
                                           .get_text()
                                           )
                    product_price = (product.find('span', {'class': 'special-price'})
                                     .find('span', {'class': 'price'})
                                     .get_text()
                                     )
                else:
                    product_price = (product.find('span', {'class': 'price-container price-final_price tax'})
                                     .find('span', {'class': 'price'})
                                     .get_text()
                                     )
                    product_price_lista = product_price

                if 'U$S' in product_price:
                    price_currency = 'U$S'
                else:
                    price_currency = '$'

                product_price = float(product_price.replace('U$S', '')
                                                   .replace('$', '')
                                                   .replace('U$S', '')
                                                   .replace(',', '.')
                                                   .replace('.', '')
                                      )
                product_price_lista = float(product_price_lista.replace('U$S', '')
                                                               .replace('$', '')
                                                               .replace('U$S', '')
                                                               .replace(',', '.')
                                                               .replace('.', '')
                                            )
                if product_price_lista == 1000000:
                    logging.warning('Price_no_discount modified because of 1MM value')
                    product_price_lista = product_price
                else:
                    product_price_lista = product_price_lista

                execution_date = datetime.utcnow()
                execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

                store.append(store_name)
                timestamp.append(execution_date)
                url.append(product_link)
                price.append(product_price)
                price_lista.append(product_price_lista)
                product_ID.append(product_name)
                currency.append(price_currency)
            logging.info('        Scraping completed for category {}'.format(URL))

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








