# from bs4 import BeautifulSoup
# import requests
# import asyncio
# import aiohttp


# def get_page_content(URL: str) -> bytes:
#     html = requests.get(URL).content
#     return html


# def get_schema(url: str) -> list:
#     html = get_page_content(url)
#     soup = BeautifulSoup(html, 'lxml')
#     print(soup.select('thead td'))
#     func = lambda x: x.text.strip() if x.text.strip() != "" else "Rank"
#     col_names = list(map(lambda x: func(x), soup.select('thead td')))
#     print(f"Schema columns: {col_names} for url: {url}")
#     return col_names


# def scrap_urls_and_flags(url: str) -> list:
#     html = get_page_content(url)
#     soup = BeautifulSoup(html, 'lxml')
#     a_tags = soup.select('li.all a')
#     urls = [(el["href"], el.text.strip()) for el in a_tags if int(el.text.strip().split("/")[0]) >= 2000]
#     return urls


# async def get_page(url: str, session: aiohttp.ClientSession) -> str:
#     async with session.get(url=url) as response:
#         html = await response.text()
#         return html


# async def get_data(url: str, session: aiohttp.ClientSession) -> list:
#     html = await get_page(url, session)
#     soup = BeautifulSoup(html, 'lxml')
#     records = soup.select('tbody tr')
#     func = lambda x: [el.text.strip() for el in x if el.text.strip()]
#     data = map(lambda x: func(x), records)
#     return list(data)


# async def main_crawler(urls: list):
#     async with aiohttp.ClientSession() as session:
#         tasks = [get_data(url[0], session) for url in urls]
#         data = await asyncio.gather(*tasks)
#         return data

# env variables
# URL = "https://hoopshype.com/salaries/players/"

# def load_table(url_table: tuple, data: list) -> None:
#     schema = create_table_schema(get_schema(url_table[0]))
#     if len(schema.fields) < 4:
#         logger.info("Page does not contains a data for period")
#         return
#     df = create_dataframe(data, schema, spark, url_table[1])
#     df = transform_df_salaries(df)
#     logger.info(f"Number of records pulled {df.count()}")
#     if not df.isEmpty():
#         if spark.catalog.tableExists("bronze.salaries"):
#             df.writeTo("bronze.salaries").partitionedBy("season").overwritePartitions()
#         else:
#             df.writeTo("bronze.salaries").partitionedBy("season").createOrReplace()
#     else:
#         logger.info("Empty table pulled")
 

# urls_flags = sorted(list(set(scrap_urls_and_flags(URL))), key=lambda x: x[1], reverse=True)
# data = asyncio.run(main_crawler(urls_flags))
# spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# for url_table, data in zip(urls_flags, data):
#     load_table(url_table, data)
