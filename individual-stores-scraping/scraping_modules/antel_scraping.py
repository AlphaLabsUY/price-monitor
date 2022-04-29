from utils import export_data_to_bigquery_csv
from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import requests
import logging
from google.cloud import bigquery


def antel(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'ANTEL'

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name, datetime.now()))

    url = 'https://tienda.antel.com.uy/catalogo/celulares-tablets'

    driver.get(url)

    time.sleep(2)
    scroll_pause_time = 1
    screen_height = driver.execute_script("return window.screen.height;")
    i = 1

    while True:

        driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
        i += 1
        time.sleep(scroll_pause_time)

        scroll_height = driver.execute_script("return document.body.scrollHeight;")

        if screen_height * i * 0.75 > scroll_height:
            break

    logging.info('       Finished Infinite Scroll for {} after {} scrolls at {}'.format(url, i, datetime.now()))

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    logging.info('       Webdriver closed after downloading HTML sourcecode for each page')

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for categories that need scroll for {} at {}'.format(store_name, datetime.now()))

    all_products = soup.find('div', {"class": "row rowEquipos promoCatalogoMobile"}).find_all('div',
                                                                                              {'class': "contBottom"})
    for product in all_products:
        product_name = product.find('a')['title']
        product_link = product.find('a')['href']

        product_price = product.find('h3').get_text()

        if 'A solo' in product_price:
            product_price = product_price.replace('A solo\n\t\n\t\t\t\t\t\t', '').replace('\n', '').split()
            price_currency = product_price[0]
            product_price = float(product_price[1].replace('.', ''))
            product_price_lista = product_price
        else:

            product_price = product.find('p', {'class': 'ptcptf'}).get_text().split()

            price_currency = product_price[1]

            product_price_lista = float(product_price[5].replace('.', ''))
            product_price = product_price_lista

        execution_date = datetime.utcnow()
        execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

        store.append(store_name)
        product_ID.append(product_name)
        price.append(product_price)
        price_lista.append(product_price_lista)
        currency.append(price_currency)
        url.append(product_link)
        timestamp.append(execution_date)

    logging.info('       Scraping ended for caregories that need scroll at {}'.format(datetime.now()))
    logging.info('---------------------------------------------------------------------------------')
    logging.info('Scraping set to Start with BS for caregories that dont need scroll at {}'.format(datetime.now()))

    for category in ['https://tienda.antel.com.uy/catalogo/informatica',
                     'https://tienda.antel.com.uy/catalogo/tv-audio-entretenimiento',
                     ]:

        content = requests.get(category)
        soup = BeautifulSoup(content.text, 'html.parser')

        all_products = soup.find('div', {"class": "row rowEquipos promoCatalogoMobile"}).find_all('div',
                                                                                                  {'class': "contBottom"})
        for product in all_products:
            product_name = product.find('a')['title']
            product_link = product.find('a')['href']

            product_price = product.find('h3').get_text()

            if 'A solo' in product_price:
                product_price = product_price.replace('A solo\n\t\n\t\t\t\t\t\t', '').replace('\n', '').split()
                price_currency = product_price[0]
                product_price = float(product_price[1].replace('.', ''))
                product_price_lista = product_price
            else:

                product_price = product.find('p', {'class': 'ptcptf'}).get_text().split()

                price_currency = product_price[1]

                product_price_lista = float(product_price[5].replace('.', ''))
                product_price = product_price_lista

            exectution_date = datetime.now()
            execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

            store.append('ANTEL')
            product_ID.append(product_name)
            price.append(product_price)
            price_lista.append(product_price_lista)
            currency.append(price_currency)
            url.append(product_link)
            timestamp.append(execution_date)
        logging.info('       Scraping completed for {} at {}'.format(category, datetime.now()))

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
    logging.warning('Dropped {} duplicated Items'.format(dropped_items))

    logging.info('Pandas DataFrame correctly created')

    export_data_to_bigquery_csv(df=df_final,
                                table_id=table_id,
                                store_name=store_name,
                                csv_path=csv_path,
                                )
