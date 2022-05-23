import os
import logging

from urllib.parse import urlparse
import requests, zipfile
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


os.chdir('/opt/airflow/dags')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('usda_ingest.log')
file_handler.setFormatter((formatter))

logger.addHandler((file_handler))

# TODO: automate web-scraping for file url
usda_file_urls = ['https://www.fsa.usda.gov/Assets/USDA-FSA-Public/usdafiles/NewsRoom/eFOIA/crop-acre-data/zips/2020-crop-acre-data/2020_fsa_acres_as_of_010521-BbO60I.zip'
    ,'https://www.fsa.usda.gov/Assets/USDA-FSA-Public/usdafiles/NewsRoom/eFOIA/crop-acre-data/zips/2019-crop-acre-data/2019_fsa_acres_jan2020_stlno1.zip'
    ,'https://www.fsa.usda.gov/Assets/USDA-FSA-Public/usdafiles/NewsRoom/eFOIA/crop-acre-data/zips/2019-crop-acre-data/2018_fsa_acres_Jan-2019-kgcr.zip'
    #,'https://www.fsa.usda.gov/Assets/USDA-FSA-Public/usdafiles/NewsRoom/eFOIA/crop-acre-data/zips/2021-crop-acre-data/2021_fsa_acres_web_as_of_080221.zip'
    ]


def download_usda_files(url_list):
    """Download and unzip USDA files"""

    WORKING_DIR = "/opt/airflow/dags/"
    os.chdir(WORKING_DIR)

    # remove existing xlsx files
    os.system(f"rm {WORKING_DIR}*.xlsx")

    logger.info('Current files before download: {}'.format(os.listdir(WORKING_DIR)))

    for zip_url in usda_file_urls:

        zip_file_name = os.path.basename(urlparse(zip_url).path)

        logger.info('Downloading file : {}'.format(zip_file_name))
        
        # unzip files
        with urlopen(zip_url) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(WORKING_DIR)
   
    logger.info('Current files after download: {}'.format(os.listdir(WORKING_DIR)))