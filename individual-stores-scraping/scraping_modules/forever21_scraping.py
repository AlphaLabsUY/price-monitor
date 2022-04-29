from utils import export_data_to_bigquery_csv
from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import logging
from google.cloud import bigquery


def forever21(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Forever 21'

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name, datetime.now()))

    categories = [
        {'url': 'https://www.forever21.com.uy/mujer?order=OrderByPriceASC&page={}',
         'pages': [1]},
        # {'url': 'https://www.forever21.com.uy/plus?order=OrderByPriceASC&page={}', 'pages': [1]},
        # {'url': 'https://www.forever21.com.uy/hombre?order=OrderByPriceASC&page={}', 'pages': [1, 2, 3], },
        # {'url': 'https://www.forever21.com.uy/nina?order=OrderByPriceASC&page={}', 'pages': [1, 2]}
    ]

    html_dictionary = {}

    errors_count = 0
    for category in categories:
        for page in category['pages']:
            try:
                driver.get(category['url'].format(page))
                time.sleep(8)
                html_dictionary['soup_{}'.format(category['url'].format(page))] = BeautifulSoup(driver.page_source,
                                                                                                'html.parser')
            except Exception as exception:
                errors_count += 1
                logging.error('-> Error to load Page {}\n'.format(category['url'].format(page)),
                              exception
                              )
                if errors_count < 6:
                    continue
                else:
                    driver.quit()
                    logging.error('Problems found when loading the pages. More that 6 pages did not load correctly')
                    raise KeyError()

    driver.quit()

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    for category in html_dictionary:

        soup = html_dictionary[category]

        if soup.find('div', {'class': "vtex-search-result-3-x-gallery "
                                      "flex flex-row flex-wrap items-stretch"
                                      " bn ph1 na4 pl9-l"}) is not None:
            all_products = soup\
                .find('div', {'class': "vtex-search-result-3-x-gallery flex "
                                       "flex-row flex-wrap items-stretch bn ph1 na4 pl9-l"})\
                .find_all('article', {'class': "vtex-product-summary-2-x-element"
                                               " pointer pt3 pb4 flex flex-column h-100"})
        else:
            logging.error('-> Error to load Page {}\n'
                          '                            '.format(category))
            continue

        for product in all_products:
            product_name = product.find('span', {
                "class": "vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body"}).get_text()
            product_link = None

            product_price = product.find('div', {
                'class': 'vtex-store-components-3-x-priceContainer vtex-product-summary-2-x'
                         '-productPriceClass flex flex-column justify-start vtex-product-summary'
                         '-2-x-price_className'})

            if product_price is not None:
                product_price = product_price.get_text()

                product_prices = (product_price.replace('$', '').replace('12 x de', '')
                                                                .replace('sin interés', '')
                                                                .replace('De', '')
                                                                .replace('De$', '')
                                                                .replace('Para', '')
                                                                .replace('Para$', '')
                                                                .replace('O', '')).split()

                if '$' in product.find('div', {'class': 'vtex-store-components-3-x-priceContainer vtex-product'
                                                        '-summary-2-x-productPriceClass flex flex-column justify'
                                                        '-start vtex-product-summary-2-x-price_className'}).get_text():
                    price_currency = '$'
                else:
                    price_currency = 'U$S'

                if (len(product_prices) == 2) | (len(product_prices) == 5):
                    product_price = float(product_prices[0])
                    product_price_lista = float(product_price)

                if (len(product_prices) == 3) | (len(product_prices) == 6):
                    product_price = float(product_prices[1])
                    product_price_lista = float(product_prices[0])

            else:
                product_price = float(0)
                product_price_lista = float(0)
                price_currency = ''
            print(product_name, product_price, product_price_lista)
            execution_date = datetime.utcnow()
            execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

            store.append('Forever 21')
            product_ID.append(product_name)
            price.append(product_price)
            price_lista.append(product_price_lista)
            currency.append(price_currency)
            url.append(product_link)
            timestamp.append(execution_date)
        logging.info('        Scraping completed for {} at {}'.format(category, datetime.now()))

    logging.info('***************************************************************************************** \n'
                 '                                                   Scraping completed for {} at {}              \n'
                 '                                 *************************************************************'
                 '****************************'
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

    logging.info('Pandas DataFrame correctly created')

    export_data_to_bigquery_csv(df=df_final,
                                table_id=table_id,
                                store_name=store_name,
                                csv_path=csv_path,
                                )