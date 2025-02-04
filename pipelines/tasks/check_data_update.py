import logging
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import locale
import build_database

"""todo
1, request site get the date
  - if the date is newer than search further
2, get the date of laste mise a jour of duckdb
3, if the date is newer than the date in db, call the build data base with the right url
"""

logger = logging.getLogger(__name__)


def check_data_update():
    # URL to monitor
    URL = "https://www.data.gouv.fr/fr/datasets/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/"
    try:
        driver = webdriver.Chrome()  # or webdriver.Firefox(), etc.

        driver.get(URL)
        # get the date from url page
        ele = driver.find_element(By.XPATH, "//h2[@class='subtitle fr-mt-3v fr-mb-1v']" )
        next_sibling = ele.find_element(By.XPATH, 'following-sibling::p[1]')
        date_str = next_sibling.text
        locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
        web_update_time = datetime.strptime(date_str, "%d %B %Y")
        # todo: replaced with db date
        db_update_time =  datetime.now()
        if db_update_time < web_update_time:
            print("data need to be updated")
        driver.close()
    except Exception as e:
        logging.error(f"Error fetch the page: {e}")


def execute():
    check_data_update()