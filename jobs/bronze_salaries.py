import logging
from spark_session import create_spark_session
from spark_transform import create_table_schema, transform_df_salaries
from web_scraper_extract_salaries import get_html_content, get_urls_and_seasons, get_column_names, generate_dataset

import pathlib

BASEDIR = pathlib.Path(__name__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)



URL = "https://www.espn.com/nba/salaries"
URL_PATTERN = "https://www.espn.com/nba/salaries/_/year/{}"


spark = create_spark_session()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

html = get_html_content(URL)["solution"]["response"]
urls_seasons = get_urls_and_seasons(html)
schema = create_table_schema(get_column_names(html) + ["season"])


for data in generate_dataset(urls_seasons=urls_seasons):
    df = spark.createDataFrame(data=data, schema=schema)
    df = transform_df_salaries(df)
    logger.info(f"Number of records pulled {df.count()}")
    if not df.isEmpty():
        if spark.catalog.tableExists("bronze.salaries"):
            df.writeTo("bronze.salaries").partitionedBy("season").overwritePartitions()
        else:
            df.writeTo("bronze.salaries").partitionedBy("season").createOrReplace()
    else:
        logger.info("Empty table pulled")
