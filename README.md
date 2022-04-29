# Price Monitor - Alphalabs

## Scraping Status:
* Fenicio :white_check_mark:
* MERCADOLIBRE :white_check_mark:
---
* Tienda Inglesa :x:
* Disco :x:
---
* Allie Store :x:
* ANTEL :white_check_mark:
* ARREDO :white_check_mark:
* CANVA :white_check_mark:
* Carestino :x:
* DAKAR :white_check_mark:
* DIMM! :white_check_mark:
* Dishershop :x:
* Feria Máxima :white_check_mark:
* Forever 21 :x: (eCommerce discontinuado)
* La Tentacion :white_check_mark:
* Mercado Viajes :white_check_mark:
* Mis Beneficios :white_check_mark:
* Movistar :white_check_mark:
* Mundo Tecno :white_check_mark:
* Narvaja :white_check_mark:
* PRÜNE :x:
* RAPSODIA :x:
* SODIMAC :white_check_mark:
* Voy De Shopping :x: (eCommerce discontinuado)
* Woow! :white_check_mark:
* XPRO :white_check_mark:

## Set Up

1. Download [Chrome Driver](https://chromedriver.chromium.org/downloads) for current Chrome Version.

2. Install pricing-master package: 

````bash
pip install -e pricing-master -q   
````
3. If working on MacOS, remove chromedriver from quaretine:

````bash
xattr -d com.apple.quarantine /Path/to/chromedriver     
````

## Run Pipeline

### Individual Stores & Fenicio:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json && 
python individual-stores-scraping/main.py
```

### Mercadolibre:

```bash
python mercadolibre/main.py \
    -s stores_file.txt \
    -o /Users/nicolasfornasari/PycharmProjects/price_monitor \
    -c /Users/nicolasfornasari/PycharmProjects/price_monitor/price-monitor-alphalabs/credentials/mercadolibre-test-credentials.json
``` 
