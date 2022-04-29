from utils import LoadData
from scraping_modules.fenicio_scraping import fenicio
from scraping_modules.carestino_scraping import carestino
from scraping_modules.dakar_scraping import dakar
from scraping_modules.dimm_scraping import dimm
from scraping_modules.la_tentacion_scraping import la_tentacion
from scraping_modules.mis_beneficios_scraping import mis_beneficios
from scraping_modules.canva_scraping import canva
from scraping_modules.xpro_scraping import xpro
from scraping_modules.feria_maxima_scraping import feria_maxima
from scraping_modules.disershop_scraping import disershop
from scraping_modules.arredo_scraping import arredo
from scraping_modules.antel_scraping import antel
from scraping_modules.movistar_scraping import movistar
from scraping_modules.mercadoviajes_scraping import mercadoviajes
from scraping_modules.forever21_scraping import forever21
from scraping_modules.mundotecno_scraping import mundotecno
from scraping_modules.narvaja_scraping import narvaja
from scraping_modules.voy_de_shopping_scraping import voydeshopping
from scraping_modules.sodimac_scraping import sodimac
from scraping_modules.prune_scraping import prune
from scraping_modules.allie_scraping import allie
from scraping_modules.rapsodia_scraping import rapsodia
from scraping_modules.woow_scraping import woow

from datetime import datetime
import time
import traceback
import logging
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


if __name__ == '__main__':

    chrome_driver_path = "/Users/nicolasfornasari/OneDrive/19_ALPHALABS/webdriver/chromedriver_93"
    table_id = 'mercadolibre-test-289117.test_set_2021.test_set'

    start_date = datetime.now()

    functions = [
        # woow,
        fenicio,
        # dakar,
        # dimm,
        # la_tentacion,
        # canva,
        # xpro,
        # feria_maxima,
        # arredo,
        # antel,
        # movistar,
        # mundotecno,
        # narvaja,
        # mercadoviajes,
        # sodimac,
        # mis_beneficios,
    ]

    failed_functions = []
    attempts_limit = 5

    for function in functions:
        attempts = 0
        while attempts < attempts_limit:
            try:
                df = function(webdriver_path=chrome_driver_path,
                              table_id=table_id)

                LoadData(df=df,
                         table_id=table_id).load_data_to_csv('fenicio.csv')
                break

            except:
                logging.warning(f'{function} failed for the {attempts+1} time')
                logging.error(traceback.format_exc())

                attempts += 1
                if attempts == attempts_limit:
                    failed_functions.append(function)
                else:
                    time.sleep(attempts*3)
                continue

    end_date = datetime.now()
    timedelta = ((end_date - start_date).total_seconds())/60
    logging.info(f'Process completed after {timedelta} minutes')

    for function_error in failed_functions:
        logging.warning(f'{function_error} Failed to perform the scraping')
