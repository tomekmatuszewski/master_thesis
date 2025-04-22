from bs4 import BeautifulSoup
import requests
import asyncio
import time
import aiohttp

PROXY_URL = "http://flaresolverr:8191/v1"

def get_page_content(url: str) -> bytes:
    html = requests.get(url).text
    return html

def get_schema(url: str) -> list:
    html = get_page_content(url)
    soup = BeautifulSoup(html, 'lxml')
    col_names = map(lambda x: x.text, soup.select('th'))
    return list(col_names)

def scrap_urls_and_flags(url: str) -> list:
    html = get_page_content(url)
    soup = BeautifulSoup(html, 'lxml')
    url_pattern = "https://hashtagbasketball.com{}"
    urls = list(map(lambda x: (url_pattern.format(x['href']), x.text), soup.select('td a')))
    return urls

async def get_page(url: str, session: aiohttp.ClientSession) -> str:
    data = {
        "cmd": "request.get",
        "url": url
    }
    headers = {
        "Content-Type": "application/json"
    }
    async with session.post(url=url, json=data, headers=headers) as response:
        html = await response.text()
        return html

def filter_data(data_record: list) -> list:
    return list(map(lambda x: x.select_one('span').text if x.find('span') else x.text, data_record))

async def get_data(url: str, session: aiohttp.ClientSession) -> list:
    html = await get_page(url, session)
    soup = BeautifulSoup(html, 'lxml')
    records = list(filter(lambda x: x.find('td'), soup.select('tr')))
    data = list(map(lambda x: x.select('td'), records))
    filtered_data = list(map(lambda x: filter_data(x), data))
    return filtered_data


async def main_crawler(urls: list):
    async with aiohttp.ClientSession() as session:
        tasks = [get_data(url[0], session) for url in urls]
        data = await asyncio.gather(*tasks)
        return data


def main_crawler_sync(urls: list):
    final_data = []
    time.sleep(2)
    for url in urls:
        json_body = {
            "cmd": "request.get",
            "url": url[0]
        }
        headers = {"Content-Type": "application/json"}
        html = requests.post(PROXY_URL, json=json_body, headers=headers).json()["solution"]['response']
        soup = BeautifulSoup(html, 'lxml')
        records = list(filter(lambda x: x.find('td'), soup.select('tr')))
        data = list(map(lambda x: x.select('td'), records))
        filtered_data = list(map(lambda x: filter_data(x), data))
        final_data.append(filtered_data)
    return final_data

