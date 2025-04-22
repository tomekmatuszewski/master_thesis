import logging
from spark_session import create_spark_session
from pathlib import Path
from web_scraper_extract_stats import get_stats_data, get_urls_and_table_names, get_schema_stats
from spark_transform import create_table_schema, create_stats_df

from selenium.webdriver.chrome.service import Service

BASEDIR = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# env variables
STATS_DATA_URL = "https://www.espn.com/nba/stats/player"
URL_STATS_PATTERN_SEASON = "https://www.espn.com/nba/stats/player/_/season/{}/seasontype/{}"

spark = create_spark_session()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def load_table(url: str, table: str, service) -> None:
    column_names, data = get_schema_stats(url), get_stats_data(url, service)
    schema = create_table_schema(column_names)
    df = create_stats_df(data, schema, spark, table)
    if not df.isEmpty():
        if spark.catalog.tableExists("bronze.stats"):
            logger.info("Table exists, adding partition")
            df.writeTo("bronze.stats").partitionedBy("season", "season_phase").overwritePartitions()
        else:
            logger.info("Table doesn't exists, creating table")
            df.writeTo("bronze.stats").partitionedBy("season", "season_phase").createOrReplace()
    else:
        logger.info("Empty table pulled")
 


urls = get_urls_and_table_names(STATS_DATA_URL, URL_STATS_PATTERN_SEASON)
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# install chrome driver used by selenium
service = Service(executable_path='/usr/bin/chromedriver')

for url, table in urls:
    load_table(url, table, service)
