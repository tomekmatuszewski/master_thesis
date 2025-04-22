from spark_session import create_spark_session
import pandas as pd
from pyspark.sql.functions import pandas_udf

SILVER_SCHEMA = "silver"
DIM_INJURY = "dim_injuries"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")

@pandas_udf("date")
def convert_to_date(date_series: pd.Series) -> pd.Series:
    return pd.to_datetime(date_series, format="%d %B %Y")

spark.udf.register("convert_to_date", convert_to_date)

spark.sql(
   f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.{DIM_INJURY} AS
    with with_sk as (
        SELECT
            monotonically_increasing_id() as SK,
            *
        FROM bronze.injuries
    ),
    cleaned as (
        SELECT 
            SK,
            PLAYER as Player,
            TEAM as team,
            convert_to_date(`INJURED ON`) as injured_on,
            convert_to_date(RETURNED) as returned,
            `DAYS MISSED` as days_missed,
            injury
        FROM with_sk
    )
    SELECT * FROM cleaned
    """
)