from spark_session import create_spark_session

SILVER_SCHEMA = "silver"
DIM_AWARDS = "dim_awards"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")

spark.sql(
   f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.{DIM_AWARDS} AS
    with with_sk as (
        SELECT
            monotonically_increasing_id() as SK,
            *
        FROM bronze.awards
    ),
    cleaned as (
        SELECT 
            SK,
            split(RECIPIENT, ",")[0] as Player,
            LAST_VALUE(AWARD, TRUE) OVER (ORDER BY SK ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Award,
            STATS as Stats,
            concat(CAST(CAST(season as INT) - 1 as STRING), "_", substring(season, 3,4)) as season
        FROM with_sk
    )
    SELECT * FROM cleaned
    """
)