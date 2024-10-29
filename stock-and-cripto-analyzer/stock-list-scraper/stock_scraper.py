import time

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class StockScraper:
    def __init__(
        self,
        service: Service,
        chrome_options: webdriver.ChromeOptions,
    ):
        self.service = service
        self.options = chrome_options
        self.driver = None

    def start_driver(self):
        self.driver = webdriver.Chrome(
            service=self.service, options=self.chrome_options
        )
        self.driver.get("https://www.nasdaq.com/market-activity/stocks/screener")
        time.sleep(10)

    def click_accept(self):
        try:
            accept_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler"))
            )
            accept_button.click()
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def wait_for_divs(self):
        try:
            div = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/div/div[3]/div[2]/div",
                    )
                )
            )
            div = WebDriverWait(div, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "nsdq-checkbox-group"))
            )
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def get_shadow_root(self):
        try:
            shadow_root = self.driver.execute_script(
                "return arguments[0].shadowRoot", self.driver
            )
            div_shadow = shadow_root.find_element(By.CLASS_NAME, "checkbox-group")
            return div_shadow.find_elements(By.CLASS_NAME, "checkbox-wrapper")
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def select_options(self, divs):
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
                return
            except Exception as e:
                self.stop_driver()
                return "ERROR!", e

    def download_csv(self):
        try:
            download_div = self.driver.find_element(
                By.XPATH,
                "/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]",
            )
            download_div.find_element(
                By.CLASS_NAME, "jupiter22-c-table__download-csv"
            ).click()
            time.sleep(20)
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def stop_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None
