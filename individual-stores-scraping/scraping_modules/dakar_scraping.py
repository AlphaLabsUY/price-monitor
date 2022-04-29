from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import logging
from google.cloud import bigquery


def dakar(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'DAKAR'

    URL = 'https://dakar.uy/categorias/{}/?orderby={}&offset=12'
    categories = ['buzos', 'calzado', 'jeans', 'pantalones', 'sastreria', 't-shirt', 'camisas', 'camperas']
    orders = ['price', 'price-desc', 'date', 'popularity']

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated at {}'.format(datetime.now()))

    url = 'https://dakar.uy/tienda/'
    driver.get(url)

    time.sleep(5)  # Allow 2 seconds for the web page to open
    scroll_pause_time = 7  # You can set your own pause time. My laptop is a bit slow so I use 1 sec
    screen_height = driver.execute_script("return window.screen.height;")   # get the screen height of the web
    i = 1

    while True:
        # scroll one screen height each time
        driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
        i += 1
        time.sleep(scroll_pause_time)
        # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
        scroll_height = driver.execute_script("return document.body.scrollHeight;")
        # Break the loop when the height we need to scroll to is larger than the total scroll height
        if screen_height * i > scroll_height:
            break

    logging.info('       Finished Infinite Scroll for {} after {} scrolls at {}'.format(url, i, datetime.now()))

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    timestamp = []
    url = []
    price = []
    price_lista = []
    store = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for {} at {}'.format(store_name, datetime.now()))

    all_products = soup.find_all('div', {'class': 'product-inner'})
    for product in all_products:

        product_name = product.find('h3', {'class': 'product-title'}).get_text().replace('\n', '')

        product_price = product.find('span',
                                     {"class": "woocommerce-Price-amount amount"})

        if product_price is not None:
            product_price = float(product_price.get_text().replace(',', '').replace('$', ''))
        else:
            product_price = None

        price_currency = product.find('span', {"class": "woocommerce-Price-currencySymbol"})
        if price_currency is not None:
            price_currency = price_currency.get_text()
        else:
            price_currency = None

        product_link = product.find('h3', {'class': 'product-title'}).find('a')['href']

        exectution_date = datetime.utcnow()
        execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

        store.append(store_name)
        product_ID.append(product_name)
        price.append(product_price)
        price_lista.append(product_price)
        currency.append(price_currency)
        url.append(product_link)
        timestamp.append(execution_date)
        print(product_name, product_price, product_link)

    logging.info('***************************************************************************************** \n'
                 '                                                      Scraping completed for {}              '
                 '                    \n'
                 '                                 *************************************************'
                 '****************************************'
                 .format(store_name))

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
