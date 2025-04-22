from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
SALARIES_STATS = "salaries_stats"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{SALARIES_STATS} AS
    with salaries_stats as (
        SELECT 
            ROUND(AVG(ds.salary),2) as avg_salary,
            ROUND(MIN(ds.salary),2) as min_salary,
            ROUND(MAX(ds.salary),2) as max_salary,
            dt.Name,
            s.season
        FROM silver.stats s
        JOIN silver.dim_salary ds ON s.SKSalary = ds.SK
        LEFT JOIN silver.dim_teams dt ON s.SKTeam = dt.SK
        GROUP BY dt.Name, s.season
        ORDER BY season DESC
    ),
    ranked as (
        SELECT 
            s.PlayerName,
            RANK() OVER(PARTITION BY dt.Name, s.season ORDER BY ds.salary DESC) as rank_salary,
            ds.salary,
            dt.Name as TeamName,
            s.season
        FROM silver.stats s
        JOIN silver.dim_salary ds ON s.SKSalary = ds.SK
        LEFT JOIN silver.dim_teams dt ON s.SKTeam = dt.SK
    ),
    ranked_highest_sal as (
        SELECT
            PlayerName,
            salary,
            TeamName,
            season
        FROM ranked
        WHERE rank_salary = 1
    )
    SELECT
        s.avg_salary,
        s.min_salary,
        s.max_salary,
        s.Name as TeamName,
        s.season,
        r.PlayerName as BestPaidPlayer
    FROM salaries_stats s
    JOIN ranked_highest_sal r ON s.max_salary = r.salary AND r.season = s.season AND r.TeamName = s.Name
    ORDER BY season DESC, max_salary DESC
"""
)