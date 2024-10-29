import time

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
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
        self.focus_div = None
        self.divs_list: list[WebElement] = []

    def start_driver(self):
        try:
            self.driver = webdriver.Chrome(service=self.service, options=self.options)
            self.driver.get("https://www.nasdaq.com/market-activity/stocks/screener")
            time.sleep(10)
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

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
            self.focus_div = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "/html/body/div[2]/div/main/div[2]/article/div/div[1]/div[2]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/div/div[3]/div[2]/div",
                    )
                )
            )
            self.focus_div = WebDriverWait(self.focus_div, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "nsdq-checkbox-group"))
            )
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def get_shadow_root(self) -> None | tuple[str, Exception]:
        try:
            if self.driver is not None:
                shadow_root = self.driver.execute_script(
                    "return arguments[0].shadowRoot", self.focus_div
                )
                div_shadow = shadow_root.find_element(By.CLASS_NAME, "checkbox-group")
                self.divs_list = div_shadow.find_elements(
                    By.CLASS_NAME, "checkbox-wrapper"
                )
                return
            raise Exception("Driver is None")
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def select_options(self):
        if self.divs_list:
            print(self.divs_list)
            try:
                for div in self.divs_list:
                    counter = 1
                    print(f"DIV {counter}:", div)
                    label = div.find_element(By.CLASS_NAME, "checkbox-label")
                    counter += 1
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
        else:
            self.stop_driver()
            return "ERROR!", "Divs not found"

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
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except Exception as e:
            self.stop_driver()
            return "ERROR!", e

    def exec_scraping_sequence(self):
        steps = [
            self.start_driver,
            self.click_accept,
            self.wait_for_divs,
            self.get_shadow_root,
            self.select_options,
            self.download_csv,
        ]

        for step in steps:
            error = step()
            if isinstance(error, tuple):
                return [error, step.__name__]
