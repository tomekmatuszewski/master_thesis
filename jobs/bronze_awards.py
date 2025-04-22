import logging
from spark_session import create_spark_session
from spark_transform import create_table_schema
from web_scraper_extract_awards import get_urls_and_seasons, get_column_names, generate_dataset
from web_scraper_commons import get_html_content
import pathlib

BASEDIR = pathlib.Path(__name__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

URL = "https://www.espn.com/nba/history/awards/_/year/2025"

spark = create_spark_session()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

html = get_html_content(URL)["solution"]["response"]
urls_seasons = get_urls_and_seasons(html)
schema = create_table_schema(get_column_names(html) + ["season"])

for data in generate_dataset(urls_seasons=urls_seasons):
    df = spark.createDataFrame(data=data, schema=schema)
    logger.info(f"Number of records pulled {df.count()}")
    if not df.isEmpty():
        if spark.catalog.tableExists("bronze.awards"):
            df.writeTo("bronze.awards").partitionedBy("season").overwritePartitions()
        else:
            df.writeTo("bronze.awards").partitionedBy("season").createOrReplace()
    else:
        logger.info("Empty table pulled")
