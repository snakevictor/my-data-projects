import pandas as pd
from selenium import webdriver
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
