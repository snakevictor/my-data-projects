from os import getcwd, listdir, path

import boto3  # type: ignore
import chromedriver_autoinstaller  # type: ignore
from stock_scraper import Service, StockScraper, webdriver

"""
Webscrapes stock names for every 10M+ company listed on  nasdaq.com and uploads it to redshift.

TODO:
* ADD CORRECT AWS ADDRESS
"""

__author__ = "Victor Monteiro Ribeiro"
__version__ = "0.9b"
__maintainer__ = "Victor Monteiro Ribeiro"
__email__ = "victormribeiro.py@gmail.com"
__status__ = "Development"


def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    client_s3 = boto3.client("s3")

    try:
        response = client_s3.upload_file(file_name, bucket, object_name)
        print(
            f"Arquivo {file_name} foi enviado para o bucket {bucket} com o nome {object_name}"
        )
        return response
    except Exception as e:
        print(e)
        raise SystemExit


def config_chrome_options(options: webdriver.ChromeOptions, directory: str):
    prefs = {
        "download.default_directory": directory + "\\",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }

    options.add_experimental_option("prefs", prefs)
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ssl-protocol=any")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-quic")
    options.add_argument("--allow-insecure-localhost")
    return options


def get_latest_file(directory: str):
    files = [path.join(directory, file) for file in listdir(directory)]
    files = [file for file in files if path.isfile(file)]
    latest_file = max(files, key=path.getmtime)
    return latest_file


def main():
    # INSTALLING CHROMEDRIVER, GETTING CURRENT DIRECTORY AND CREATING A SERVI
    chromedriver_autoinstaller.install()
    directory = getcwd()
    service = Service()

    # DEFINING CHROME OPTIONS
    chrome_options = webdriver.ChromeOptions()
    options = config_chrome_options(chrome_options, directory)

    # INITIALIZING STOCK SCRAPER
    scraper = StockScraper(service=service, chrome_options=options)
    error = scraper.exec_scraping_sequence()
    if error:
        return error

    # GETTING DOWNLOADED FILE (LATEST BY MODIFICATION DATE)
    latest_file = get_latest_file(directory)
    print(f"Arquivo {latest_file} foi baixado")

    # UPLOADING FILE TO S3
    # upload_to_s3(latest_file, "bucket", "stock-list-scraper")

    return "Success!"


if __name__ == "__main__":
    print(main())
