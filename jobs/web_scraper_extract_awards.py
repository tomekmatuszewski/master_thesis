from typing import Generator
from bs4 import BeautifulSoup
from web_scraper_commons import get_html_content, clean_url


def get_urls_and_seasons(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, 'lxml')
    options = soup.select('option')
    return list(filter(lambda x: x[0] != "#" and "_/year" in x[0] and int(x[1]) >= 2000, [(clean_url(option["value"]), option.text) for option in options]))

def get_column_names(html: str) -> list[str]:
    soup = BeautifulSoup(html, 'lxml')
    columns = [el.text for el in soup.select('table tr td')][1:4]
    return columns

def get_data(html: str, season: str) -> list[tuple[str]]:
    soup = BeautifulSoup(html, 'lxml')
    data = list(map(lambda x: None if x == "" else x, [element.text for element in soup.select('table tr td')][4:]))
    chunked = [tuple(data[element:element+3] + [season]) for element in range(0, len(data), 3)]
    return chunked

def scrap_page(url: str, season: str) -> list[tuple[str]]:
    print(f"Scraping url {url}")
    html = get_html_content(url)["solution"]["response"]
    return get_data(html, season)

def generate_dataset(urls_seasons: list[tuple[str, str]]) -> Generator[list[tuple[str]], None, None]:
    for url, season in urls_seasons:
        yield scrap_page(url, season) 