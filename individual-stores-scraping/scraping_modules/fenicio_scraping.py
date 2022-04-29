from utils import create_dataframe
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import logging


def fenicio(webdriver_path=None, table_id=None):

    logging.getLogger().setLevel(logging.INFO)

    stores = [
        # {"store": "Alien Store",
        #  "link": "https://alienstore.uy/catalogo",
        #  "link_scroll": "https://alienstore.uy/catalogo?js=1&pag={}",
        #  "pages": 32},
        {"store": "Divino Fenicio",
         "link": "https://www.divino.com.uy/catalogo",
         "link_scroll": "https://www.divino.com.uy/catalogo?ord=prd&js=1&pag={}",
         "pages": 10},
        {"store": "Route 66",
         "link": "https://route66.com.uy/catalogo",
         "link_scroll": "https://route66.com.uy/catalogo?js=1&pag={}",
         "pages": 20},
        {"store": "FORUS",
         "link": "https://www.forus.uy/catalogo",
         "link_scroll": "https://www.forus.uy/catalogo?js=1&pag={}",
         "pages": 34},
        {"store": "El Emporio del Hogar",
         "link": "https://www.elemporiodelhogar.com.uy/catalogo",
         "link_scroll": "https://www.elemporiodelhogar.com.uy/catalogo?js=1&pag={}",
         "pages": 15},
        # {"store": "La Dolfina",
        #  "link": "https://www.ladolfinapolo.com.uy/catalogo",
        #  "link_scroll": "https://www.ladolfinapolo.com.uy/catalogo?js=1&pag={}",
        #  "pages": 12},
        {"store": "Le Blanc",
         "link": "https://www.leblanc.com.uy/catalogo",
         "link_scroll": "https://www.leblanc.com.uy/catalogo?js=1&pag={}",
         "pages": 20},
        # {"store": "Legacy",
        #  "link": "https://www.legacy.com.uy/catalogo",
        #  "link_scroll": "https://www.legacy.com.uy/catalogo?js=1&pag={}",
        #  "pages": 94},
        {"store": "Piece Of Cake",
         "link": "https://www.pieceofcake.com.uy/catalogo",
         "link_scroll": "https://www.pieceofcake.com.uy/catalogo?js=1&pag={}",
         "pages": 19},
        {"store": "Mini So",
         "link": "https://www.minisouruguay.com.uy/catalogo",
         "link_scroll": "https://www.minisouruguay.com.uy/catalogo?js=1&pag={}",
         "pages": 10},
        {"store": "Symphorine",
         "link": "https://symphorine.com.uy/productos",
         "link_scroll": "https://symphorine.com.uy/productos?js=1&pag={}",
         "pages": 10},
        # {"store": "Zurra Natural Leather",
        #  "link": "https://www.zurraleather.com./catalogo",
        #  "link_scroll": "https://www.zurraleather.com./catalogo?js=1&pag={}",
        #  "pages": 4},
        {"store": "Nstore",
         "link": "https://nstore.com.uy/catalogo",
         "link_scroll": "https://nstore.com.uy/catalogo?js=1&pag={}",
         "pages": 15},
        {"store": "Universo Binario",
         "link": "https://universobinario.com/catalogo",
         "link_scroll": "https://universobinario.com/catalogo?js=1&pag={}",
         "pages": 10},
        # "pages": 217},
        {"store": "Tienda We Play",
         "link": "https://tiendaweplay.com.uy/catalogo",
         "link_scroll": "https://tiendaweplay.com.uy/catalogo?js=1&pag={}",
         "pages": 15},
        {"store": "name it",
         "link": "https://nameit.com.uy/catalogo",
         "link_scroll": "https://nameit.com.uy/catalogo?js=1&pag={}",
         "pages": 38},
        {"store": "Veroca Joyas",
         "link": "https://www.verocajoyas.com.uy/catalogo",
         "link_scroll": "https://www.verocajoyas.com.uy/catalogo?js=1&pag={}",
         "pages": 47},
        {"store": "Deco Hogar",
         "link": "https://www.decohogar.com.uy/catalogo",
         "link_scroll": "https://www.decohogar.com.uy/catalogo?js=1&pag={}",
         # "pages": 379
         "pages": 100},
        {"store": "Daniel Cassin",
         "link": "https://www.danielcassin.com.uy/catalogo",
         "link_scroll": "https://www.danielcassin.com.uy/catalogo?js=1&pag={}",
         "pages": 37},
        {"store": "Da Pie",
         "link": "https://dapie.com.uy/catalogo",
         "link_scroll": "https://dapie.com.uy/catalogo?js=1&pag={}",
         "pages": 13},
        {"store": "Boutique Erotica",
         "link": "https://www.boutiqueerotica.com.uy/catalogo",
         "link_scroll": "https://www.boutiqueerotica.com.uy/catalogo?js=1&pag={}",
         "pages": 54},
        {"store": "Basefield",
         "link": "https://www.bsf.com.uy/catalogo",
         "link_scroll": "https://www.bsf.com.uy/catalogo?js=1&pag={}",
         "pages": 12},
        {"store": "Adam Tailor",
         "link": "https://www.adamtailor.com.uy/catalogo",
         "link_scroll": "https://www.adamtailor.com.uy/catalogo?js=1&pag={}",
         "pages": 23},
        {"store": "CUATROASES",
         "link": "https://cuatroases.com.uy/catalogo",
         "link_scroll": "https://cuatroases.com.uy/catalogo?js=1&pag={}",
         "pages": 10},
        {"store": "Club House",
         "link": "https://www.clubhouse.com.uy/catalogo",
         "link_scroll": "https://www.clubhouse.com.uy/catalogo?js=1&pag={}",
         "pages": 10},
    ]

    timestamp = []
    URL = []
    price = []
    price_lista = []
    store_name = []
    product_ID = []
    currency = []

    logging.info('Scraping set to Start for Fenicio Stores at {}'.format(datetime.now()))

    for store in stores:
        store_ID = store["store"]

        for page in range(1, store["pages"] + 1):
            link = store["link_scroll"].format(page)
            content = requests.get(link)
            if content.reason == 'Not Found':
                logging.error('Error for store {} with status code {}'.format(store, content.status_code))

            soup = BeautifulSoup(content.text, 'html.parser')
            all_products = soup.find_all('div', {'data-disp': "1"})

            for product in all_products:
                exectution_date = datetime.utcnow()
                execution_date = exectution_date.strftime("%m/%d/%Y %H:%M:%S")

                product_price_venta = product.find('strong', {'class': "precio venta"})
                product_price = float(
                    product_price_venta.find('span', {'class': 'monto'}).get_text().replace('.', '').replace(',', '.'))
                product_currency = product_price_venta.find('span', {'class': 'sim'})\
                                                      .get_text()\
                                                      .replace('USD', 'U$S')\
                                                      .replace('UYU', '$')

                product_price_lista = product.find('del', {'class': "precio lista"})

                if product_price_lista is not None:
                    price_lista_ = float(
                        product_price_lista.find('span',
                                                 {'class': 'monto'}).get_text().replace('.', '').replace(',', '.'))
                else:
                    price_lista_ = product_price

                product_name = product.find('a', {'class': 'tit'}).get_text()
                product_link = product.find('a', {'class': 'tit'})['href']

                timestamp.append(execution_date)
                URL.append(product_link)
                price.append(product_price)
                store_name.append(store_ID)
                product_ID.append(product_name)
                currency.append(product_currency)
                price_lista.append(price_lista_)

        logging.info('             Completed store {} at {}'.format(store_ID, datetime.now()))

    logging.info('Scraping completed for Fenicio Stores at {}'.format(datetime.now()))

    return create_dataframe(
        timestamp=timestamp,
        URL=URL,
        price=price,
        store_name=store_name,
        product_ID=product_ID,
        currency=currency,
        price_lista=price_lista
    )



