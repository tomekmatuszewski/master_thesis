from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
SALARIES_RUNAWAY = "salaries_runaway"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{SALARIES_RUNAWAY} AS
    with highest_max as (
    SELECT 
        AVG(max_salary) avg_max,
        TeamName
    FROM gold.salaries_stats
    GROUP BY TeamName
    ORDER BY avg_max desc
    LIMIT 15
    ),
    teams as(
        SELECT 
            TeamName
        FROM highest_max
    )
    SELECT
        *
    FROM gold.salaries_stats
    WHERE TeamName in (SELECT TeamName from teams)
"""
)