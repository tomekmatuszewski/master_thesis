import logging
from spark_session import create_spark_session
from pyspark.sql.functions import row_number
from pyspark.sql import Window
from pathlib import Path

BASEDIR = Path(__file__).resolve().parent
SILVER_SCHEMA = "silver"
DIM_TEAMS = "dim_teams"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")

windowSpec = Window.orderBy("Name")
silver_teams = spark.read.table("bronze.teams").withColumn(
    "SK", row_number().over(windowSpec)
).select(
    "SK",
    "Abbreviation",
    "Name"
)
if spark.catalog.tableExists(f"{SILVER_SCHEMA}.{DIM_TEAMS}"):
    logger.info("Table exists, overwriting table")
    silver_teams.writeTo(f"{SILVER_SCHEMA}.{DIM_TEAMS}").replace()
else:
    logger.info("Table doesn't exists, creating table")
    silver_teams.writeTo(f"{SILVER_SCHEMA}.{DIM_TEAMS}").createOrReplace()
   
