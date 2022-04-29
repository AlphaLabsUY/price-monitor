from selenium import webdriver
from bs4 import BeautifulSoup
import time
from datetime import datetime
import pandas as pd
import logging
from google.cloud import bigquery


def movistar(table_id, csv_path=None, webdriver_path=None):

    logging.getLogger().setLevel(logging.INFO)
    store_name = 'Movistar'

    driver = webdriver.Chrome(webdriver_path)

    logging.info('Chrome webdriver initiated for {}'.format(store_name, datetime.now()))

    url = 'https://www.movistar.com.uy/portal/equipos'

    driver.get(url)

    while True:
        try:
            time.sleep(2)
            driver.find_element_by_xpath(
                '//*[@id="portlet_equipmentOfferings_WAR_tfnurgecommercewar"]/div/div/div/div[2]/div/div[2]/button').click()
        except:
            logging.info('       Finished Clicking to pass slides at {}'.format(datetime.now()))
            break

    time.sleep(2)

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

    logging.info('Scraping set to Start for {} at {}'.format(store_name, datetime.now()))

    all_products = (soup.find('div', {'class': 'nc-device-catalog__list jsDeviceCatalogList'})
                    .find_all('div', {"class": "nc-device-catalog__item"}))

    for product in all_products:
        product_name = product.find('div', {'class': 'nc-device__title jsSelectDeviceTitle'}).get_text()
        product_link = ('https://www.movistar.com.uy/' +
                        product.find('a', {'class': 'nc-device__click-area jsSelectDevicePanel'})['href'])
        product_price = float(product.find('span', {'class': 'nc-device__price-other-integer'})
                              .get_text()
                              .replace('$', '')
                              .replace('.', '')
                              )
        if '$' in (product.find('span', {'class': 'nc-device__price-other-integer'}).get_text()):
            price_currency = '$'
        else:
            price_currency = 'U$S'

        execution_date = datetime.utcnow()
        execution_date = execution_date.strftime("%m/%d/%Y %H:%M:%S")

        store.append(store_name)
        product_ID.append(product_name)
        price.append(product_price)
        price_lista.append(product_price)
        currency.append(price_currency)
        url.append(product_link)
        timestamp.append(execution_date)

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
