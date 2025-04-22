
from typing import Generator
from bs4 import BeautifulSoup
from web_scraper_commons import get_html_content, clean_url


def get_urls_and_seasons(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, 'lxml')
    options = soup.select('option')
    return list(filter(lambda x: x[0] != "#", [(clean_url(option["value"]), option.text) for option in options]))

def get_column_names(html: str) -> list[str]:
    soup = BeautifulSoup(html, 'lxml')
    columns = soup.select('tr td')
    seen = set()
    columns = [column.text for column in columns if column.has_attr("width")]
    return [column for column in columns if not(column in seen or seen.add(column))]

def get_data(html: str, season: str) -> list[tuple[str]]:
    soup = BeautifulSoup(html, 'lxml')
    data = [element.text for element in soup.select('tr td')]
    chunked = [tuple(data[element:element+4] + [season]) for element in range(0, len(data), 4)]
    return chunked

def get_pages(html: str) -> range:
    soup = BeautifulSoup(html, 'lxml')
    scope_nums = list(map(lambda el: int(el) + 1, soup.select_one(".page-numbers").text.split(" of ")))
    return range(scope_nums[0], scope_nums[1])

def scrap_page(url: str, season: str) -> list[tuple[str]]:
    page_data = []
    print(f"Scraping url {url}")
    html = get_html_content(url)["solution"]["response"]
    page_scope = get_pages(html)
    page_data.extend(get_data(html, season))
    for page in page_scope:
        url_page = f"{url}/_/page/{page}" if url.endswith("salaries") else f"{url}/page/{page}"
        print(f"Scraping url {url_page}")
        html = get_html_content(url_page)["solution"]["response"]
        data = get_data(html, season)
        page_data.extend(data)
    return page_data



def generate_dataset(urls_seasons: list[tuple[str, str]]) -> Generator[list[tuple[str]], None, None]:
    for url, season in urls_seasons:
        yield scrap_page(url, season) 