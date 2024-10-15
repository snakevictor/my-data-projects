import time
from os import getcwd, listdir, path

import chromedriver_autoinstaller  # type: ignore
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

"""
Webscrapes stock names for every 10M+ company listed on  nasdaq.com and uploads it to redshift.

TODO:
* 
"""

__author__ = "Victor Monteiro Ribeiro"
__version__ = "0.1"
__maintainer__ = "Victor Monteiro Ribeiro"
__email__ = "victormribeiro.py@gmail.com"
__status__ = "Development"

if __name__ == "__main__":
    # CONFIGURANDO DRIVER DO CHROME PARA SELENIUM
    chromedriver_autoinstaller.install()

    service = Service()
    chrome_options = Options()
    prefs = {
        "download.default_directory": getcwd(),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ssl-protocol=any")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-quic")

    driver = webdriver.Chrome(service=service, options=chrome_options)
    str1 = driver.capabilities["browserVersion"]
    print(str1)
    wait = WebDriverWait(driver, 30)
    print("Iniciando Selenium")
    driver.get("https://www.nasdaq.com/market-activity/stocks/screener")

    print("Aguardando carregamento da tela")
    time.sleep(10)
    # ENTRANDO NA EXECUÇÃO
    print("Entrando na execução")
    driver.find_element(By.ID, "onetrust-accept-btn-handler").click()

    try:
        driver.find_element(
            By.XPATH,
            value="""/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div/div/div/div[2]/div/div[2]/div[1]/div/div[3]/div[2]/div/nsdq-checkbox-group//div/div[1]/label/input""",
        ).click()
        driver.find_element(
            By.XPATH,
            value="""/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div/div/div/div[2]/div/div[2]/div[1]/div/div[3]/div[2]/div/nsdq-checkbox-group//div/div[2]/label/input""",
        ).click()

    except NoSuchElementException:
        print("Erro ao encontrar elemento")

    time.sleep(20)
    driver.quit()
