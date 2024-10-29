import time
from os import getcwd, listdir, path

import boto3  # type: ignore
import chromedriver_autoinstaller  # type: ignore
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

"""
Webscrapes stock names for every 10M+ company listed on  nasdaq.com and uploads it to redshift.

TODO:
* ADD CORRECT AWS ADDRESS
"""

__author__ = "Victor Monteiro Ribeiro"
__version__ = "0.9a"
__maintainer__ = "Victor Monteiro Ribeiro"
__email__ = "victormribeiro.py@gmail.com"
__status__ = "Development"


def upload_para_s3(nome_arquivo, bucket, nome_objeto=None):
    if nome_objeto is None:
        nome_objeto = nome_arquivo

    client_s3 = boto3.client("s3")

    response = client_s3.upload_file(nome_arquivo, bucket, nome_objeto)
    print(
        f"Arquivo {nome_arquivo} foi enviado para o bucket {bucket} com o nome {nome_objeto}"
    )
    return response


def scrape_stock_list():
    driver = webdriver.Chrome(service=service, options=chrome_options)
    str1 = driver.capabilities["browserVersion"]
    print(str1)
    print("Iniciando Selenium")
    driver.get("https://www.nasdaq.com/market-activity/stocks/screener")

    print("Aguardando carregamento da tela")
    time.sleep(10)
    # ENTRANDO NA EXECUÇÃO
    print("Entrando na execução")
    accept_button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler"))
    )
    accept_button.click()

    try:
        div = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    """/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/div/div[3]/div[2]/div""",
                )
            )
        )

        div = WebDriverWait(div, 10).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "nsdq-checkbox-group",
                )
            )
        )

        shadow_root = driver.execute_script("return arguments[0].shadowRoot", div)
        div_shadow = shadow_root.find_element(By.CLASS_NAME, "checkbox-group")
        divs = div_shadow.find_elements(By.CLASS_NAME, "checkbox-wrapper")

    except Exception as e:
        print("ERROR!", e)
        raise SystemExit

    for div in divs:
        label = div.find_element(By.CLASS_NAME, "checkbox-label")
        try:
            for id in ["option-mega", "option-large"]:
                if label.get_attribute("for") == id:
                    try:
                        label.find_element(
                            By.XPATH, './/input[@type="checkbox"]'
                        ).click()
                    except NoSuchElementException:
                        continue
        except Exception as e:
            print("ERROR!", e)
            raise SystemExit

    download_div = driver.find_element(
        By.XPATH,
        "/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]",
    )
    download_div.find_element(By.CLASS_NAME, "jupiter22-c-table__download-csv").click()

    time.sleep(20)
    driver.quit()
    return


if __name__ == "__main__":
    # CONFIGURANDO DRIVER DO CHROME PARA SELENIUM
    chromedriver_autoinstaller.install()

    diretorio = getcwd()

    service = Service()
    chrome_options = Options()
    print(getcwd())
    prefs = {
        "download.default_directory": diretorio + "\\",
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
    chrome_options.add_argument("--allow-insecure-localhost")

    scrape_stock_list()
    files = [path.join(diretorio, file) for file in listdir(diretorio)]
    files = [file for file in files if path.isfile(file)]
    # Find the latest file by modification time
    latest_file = max(files, key=path.getmtime)

    # Upload the latest file to S3
    upload_para_s3(latest_file, "bucket", "stock-list-scraper")
