from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests
from datetime import datetime
from google.cloud import bigquery
import logging
import pandas as pd
logging.basicConfig(level=logging.DEBUG)


def create_dataframe(timestamp,
                     URL,
                     price,
                     store_name,
                     product_ID,
                     currency,
                     price_lista):

    df = pd.DataFrame({
            "store": store_name,
            "product": product_ID,
            "price": price,
            "price_no_discount": price_lista,
            "price_currency": currency,
            "installments": None,
            "installment_payment": float(0),
            "installment_currency": '',
            "url": URL,
            "timestamp": pd.to_datetime(timestamp)
        }
    )

    len_prev_drop = len(df)
    df = df.drop_duplicates(subset=['product', 'price', 'price_no_discount'], keep='first')

    logging.warning(f'Dropped {len(df) - len_prev_drop} duplicated Items')

    return df


class LoadData:
    """
    This Class will handle the upload of data to BigQuery or
    store data locally in CSV
    """

    def __init__(self,
                 table_id,
                 df):

        self.client = bigquery.Client()
        self.job_config = bigquery.LoadJobConfig(
            schema=[
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
            ]
        )
        self.table_id = table_id
        self.df = df

    def load_df_to_bigquery(self):

        job = self.client.load_table_from_dataframe(
            dataframe=self.df,
            destination=self.table_id,
            job_config=self.job_config)

        job.result()

        logging.info(f'Finished to load Data into BigQuery at {datetime.now()}')

    def load_data_to_csv(self, csv_path):

        self.df.to_csv(csv_path, encoding='utf-8-sig')
        logging.info('Finished to load Data into CSV at {}'.format(datetime.now()))


def get_html(url,
             max_retries=4,
             backoff_factor=2):

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[400, 401, 403, 404, 406, 408, 429,
                          451, 500, 502, 503, 504, 522, 524],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()

    http.mount("https://", adapter)
    http.mount("http://", adapter)

    headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0'
                             'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                             ' Chrome/84.0.4147.125 Safari/537.36'}

    content = http.get(url, headers=headers)

    return content


def export_data_to_bigquery_csv(df, table_id, store_name, csv_path):

    client = bigquery.Client()
    logging.info('Initiated BigQuery Client: {}'.format(client))

    table_id = table_id
    csv_path = csv_path

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

    job = client.load_table_from_dataframe(df,
                                           table_id,
                                           job_config=job_config)
    job.result()

    logging.info('Finished to load Data into BigQuery')

    date = datetime.now()
    date = date.strftime("%m%d%Y")
    df.to_csv(csv_path.format(store_name, date), encoding='utf-8-sig')

    logging.info('Finished to load Data into CSV at {}'.format(datetime.now()))
