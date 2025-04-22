from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, lit, regexp_replace
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from pathlib import Path
import logging

logging.basicConfig(
     level=logging.INFO,
     format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")


def create_table_schema(column_names: list) -> StructType:
    schema = map(lambda x: StructField(x, StringType(), True), column_names)
    return StructType(list(schema))


def create_dataframe(data: list, schema: StructType, spark: SparkSession, flag: str):
    logger.info("Creating dataframe")
    df = spark.createDataFrame(data=data, schema=schema)
    df = df.withColumn('flag', lit(flag))
    return df

def create_stats_df(data: list, schema: StructType, spark: SparkSession, flag: str) -> DataFrame:
    logger.info("Creating dataframe")
    df = spark.createDataFrame(data=data, schema=schema)
    flag_elems = flag.split("_")
    if flag.startswith("Postseason"):
        df = df.withColumns(
            {
                "season": lit(f"{flag_elems[1]}_{flag_elems[2]}"),
                "season_phase": lit("Post")
            }
        )
    else:
        df = df.withColumns(
            {
                "season": lit(f"{flag_elems[2]}_{flag_elems[3]}"),
                "season_phase": lit("Regular")
            }
        )
    return df

def get_converted_stats_df(df: DataFrame) -> DataFrame:
    logger.info("Data types cleaning for")
    integer_fields = ["RK", "GP", "DD2", "TD3"]
    string_fields = ["Name", "POS", "flag"]
    for column in df.columns:
        if column in integer_fields:
            df = df.withColumn(column, col(column).cast(IntegerType()))
        elif column in string_fields:
            continue
        else:
            df = df.withColumn(column, col(column).cast(FloatType()))
    return df


def transform_injuries_df(df: DataFrame):
    df = df.filter(col('PLAYER') != 'PLAYER').withColumn("injury", col("flag")).drop("flag")
    return df

def transform_df_salaries(df: DataFrame):
    df = df.filter(col('NAME') != 'NAME').withColumn("season", regexp_replace("season", "-", "_"))
    return df

# def transform_df_salaries(df: DataFrame) -> DataFrame:
#     df = df.withColumn("season", regexp_replace("flag", "/", "_"))
#     fields_to_remove = [df.columns[index] for index in range(len(df.columns)) if index > 3 and
#                         df.columns[index] != "season"]
#     if fields_to_remove:
#         df = df.drop(*fields_to_remove)
#     print("Columns", df.columns)
#     df = df.withColumnsRenamed(
#             {
#                 df.columns[2]: "salary",
#                 df.columns[3]: "current_salary_inflation"
#             }
#         )
#     return df


