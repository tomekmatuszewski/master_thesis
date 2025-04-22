from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
LAST_FIVE_SEASON_MOST_INJURED_TEAMS = "last_five_season_most_injured_teams"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {LAST_FIVE_SEASON_MOST_INJURED_TEAMS}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{LAST_FIVE_SEASON_MOST_INJURED_TEAMS} AS
    with last_5_season_avg_inj as(
        SELECT 
            AVG(avg_days_missed_by_player) as highest_avg_days_missed,
            TeamName
        FROM gold.avg_days_missed_by_team_player  
        where season <= '2023_24' and season >= '2019_20'
        GROUP BY TeamName
        ORDER BY highest_avg_days_missed DESC
        ),
        top_injured_10 as (
            SELECT
                TeamName
            FROM last_5_season_avg_inj
            LIMIT 10
        )
        SELECT
            *
        FROM gold.avg_days_missed_by_team_player
        WHERE TeamName in (SELECT TeamName from top_injured_10)
        AND season <= '2023_24' AND season >= '2019_20'
        ORDER BY season DESC, avg_days_missed_by_player DESC
"""
)