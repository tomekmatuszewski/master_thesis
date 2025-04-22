import logging
from spark_session import create_spark_session
from pyspark.sql.functions import row_number, to_date
from pyspark.sql import Window
from pathlib import Path

BASEDIR = Path(__file__).resolve().parent
SILVER_SCHEMA = "silver"
DIM_TEAMS = "dim_seasons"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")

windowSpec = Window.orderBy("season")
silver_teams = spark.read.table("bronze.seasons").withColumn(
    "SK", row_number().over(windowSpec)
).select(
    "SK",
    "season",
    to_date("season_start", 'dd-MM-yyyy').alias("season_start"),
    to_date("season_end", 'dd-MM-yyyy').alias("season_end"),
    to_date("playoff_start_date", 'dd-MM-yyyy').alias("playoff_start_date"),
    to_date("finals_start_date", 'dd-MM-yyyy').alias("finals_start_date"),
    to_date("finals_end_date", 'dd-MM-yyyy').alias("finals_end_date"),
    "season_type",
    "season_length_days",
    "champion",
    "runner_up"
)

if spark.catalog.tableExists(f"{SILVER_SCHEMA}.{DIM_TEAMS}"):
    logger.info("Table exists, overwriting table")
    silver_teams.writeTo(f"{SILVER_SCHEMA}.{DIM_TEAMS}").replace()
else:
    logger.info("Table doesn't exists, creating table")
    silver_teams.writeTo(f"{SILVER_SCHEMA}.{DIM_TEAMS}").createOrReplace()
   
