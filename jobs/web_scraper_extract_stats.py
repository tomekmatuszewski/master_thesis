from bs4 import BeautifulSoup
import re
import logging
from functools import reduce
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
from selenium.webdriver.chromium.webdriver import ChromiumDriver
from selenium.webdriver.chromium.options import ChromiumOptions
from web_scraper_commons import get_html_content
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent
PROXY_URL = "http://flaresolverr:8191/v1"

logging.basicConfig(
     level=logging.INFO,
     format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )
logger = logging.getLogger(__name__)


class ChromeDriver:

    def __init__(self, url, service):
        self.url = url
        self.options = ChromiumOptions()
        self.add_options_arg()
        self.service = service
        self.driver = ChromiumDriver(options=self.options, service=self.service)
        self.driver.get(self.url)

    def add_options_arg(self):
        self.options.add_argument("--headless")
        # self.options.add_argument('--no-sandbox')
        # self.options.add_argument("--disable-dev-shm-usage")
        # self.options.add_argument("--disable-browser-side-navigation")
        # self.options.add_argument("--disable-gpu")

    def __enter__(self):
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()


def parse_table_name(table_name: str) -> str:
    table_name = "_".join(table_name.split(" ")[::-1]).replace("-", "_")
    return table_name


def get_urls_and_table_names(url: str, url_pattern: str):
    html = get_html_content(url)
    soup = BeautifulSoup(html, 'lxml')
    options = soup.select('option')
    # options = reduce(lambda x, y: x+y, list(map(lambda x: x.select('option'), select_tags)))
    filtered_options = list(filter(lambda x: x.has_attr("data-param-value") \
                                             and re.match(r'\\"\d{4}|\d{1}\\"', x["value"]), options))
    urls = list(map(lambda x: (x["value"].split("|"), x.text), filtered_options))
    return list(map(lambda x: (url_pattern.format(x[0][0].replace('\\"', ""), x[0][1].replace('\\"', "")), parse_table_name(x[1])), urls))


def get_schema_stats(url: str):
    logger.info(f"Getting schema for url: {url}")
    html = get_html_content(url)
    soup = BeautifulSoup(html, 'lxml')
    headers = soup.select('th')
    return list(map(lambda x: x.text, headers))


def get_page_with_selenium(url: str, service) -> str:
    logger.info("Scraping page using selenium")
    with ChromeDriver(url, service) as driver:
        while True:
            try:
                link = driver.find_element(By.LINK_TEXT, 'Show More')
                driver.execute_script("arguments[0].click();", link)
                time.sleep(3)
            except NoSuchElementException as e:
                logger.info("Full Table Loaded")
                break
        time.sleep(2)
        return driver.page_source


def get_stats_data_from_record(record: list) -> list:
    return [f"{el.find('a').text} {el.find('span').text}" if el.find('span') else el.text for el in record]


def get_stats_data(url: str, service) -> list:
    html = get_page_with_selenium(url, service)
    logger.info("Full page scraped, selecting table from html")
    soup = BeautifulSoup(html, 'lxml')
    table_rows = soup.select('tr.Table__TR.Table__TR--sm')
    logger.info("Getting stats data")
    data = list(map(lambda x: get_stats_data_from_record(x), table_rows))
    half = len(data) // 2
    players, stats = data[:half], data[half:]
    records = list(map(lambda x: reduce(lambda z, y: z+y, x), list(zip(players, stats))))
    return records
