import logging
from spark_session import create_spark_session
from pathlib import Path

BASEDIR = Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

spark = create_spark_session()

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
options = {"header": "true", "delimiter": ","}
logger.info("Reading teams .csv file")
df = spark.read.options(**options).csv(f"{BASEDIR}/data/seasons.csv")
if spark.catalog.tableExists("bronze.seasons"):
    logger.info("Table exists, overwriting table")
    df.writeTo("bronze.seasons").replace()
else:
    logger.info("Table doesn't exists, creating table")
    df.writeTo("bronze.seasons").createOrReplace()
   
