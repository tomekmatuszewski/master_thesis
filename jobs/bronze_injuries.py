import logging
from spark_session import create_spark_session
from web_scraper_extract_injuries import get_schema, scrap_urls_and_flags, main_crawler_sync
from spark_transform import create_table_schema, create_dataframe, transform_injuries_df


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# env variables
# URL = os.getenv('INJURIES_DATA_URL')
INJURIES_DATA_URL = "https://hashtagbasketball.com/nba-injury"

spark = create_spark_session()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def load_table(url_table: tuple, data: list) -> None:
    url , flag = url_table
    flag = flag.replace(" ", "_")
    schema = create_table_schema(get_schema(url))
    df = create_dataframe(data, schema, spark, flag)
    df = transform_injuries_df(df).persist()
    logger.info(f"Number of records pulled {df.count()}")
    if not df.isEmpty():
        if spark.catalog.tableExists("bronze.injuries"):
            df.writeTo("bronze.injuries").partitionedBy("injury").overwritePartitions()
        else:
            df.writeTo("bronze.injuries").partitionedBy("injury").createOrReplace()
    else:
        logger.info("Empty table pulled")
    df.unpersist()


urls_flags = scrap_urls_and_flags(INJURIES_DATA_URL)
data = main_crawler_sync(urls_flags)
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

for url_table, data in zip(urls_flags, data):
    load_table(url_table, data)
