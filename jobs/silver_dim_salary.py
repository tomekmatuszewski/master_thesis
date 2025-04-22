from spark_session import create_spark_session

SILVER_SCHEMA = "silver"
DIM_SALARY = "dim_salary"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SILVER_SCHEMA}.{DIM_SALARY} AS
    with with_previous as (
        SELECT
            split(NAME, ",")[0] as name,
            TEAM as team,
            SALARY as salary,
            concat(split(season, "_")[0], "_", substring(split(season, "_")[1], 3,4)) as season,
            LAG(SALARY, 1) OVER (PARTITION BY NAME ORDER BY season) as previous_salary
        FROM bronze.salaries
    ),
    with_indicators as (
        SELECT 
            *,
            CASE
                WHEN salary <> previous_salary THEN 1
                ELSE 0
            END AS change_indicator
        FROM with_previous
    ),
    with_streaks as (
        SELECT 
        *,
        SUM(change_indicator) 
            OVER(PARTITION BY name ORDER BY season) as streak_identifier
        FROM with_indicators
    ),
    final as (
    SELECT
        monotonically_increasing_id() as SK,
        name as Player,
        team,
        CAST(replace(replace(salary, "$", ""), ",", "") as decimal) as salary,
        MIN(season) as start_season,
        MAX(season) as end_season,
        (SELECT MAX(season) FROM with_previous) as current_season
    FROM with_streaks
    GROUP BY name, team, streak_identifier, salary
    ORDER BY name, streak_identifier
    )
    SELECT * FROM final
    """
)