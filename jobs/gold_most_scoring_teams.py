from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
MOST_SCORING_TEAMS = "most_scoring_teams"
TEAM_NUMBERS = 7

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{MOST_SCORING_TEAMS} AS
    with most_scoring_teams (
        SELECT 
            AVG(avg_player_points_per_game) as avg_total,
            Name as TeamName
        FROM gold.team_basic_stats_agg
        GROUP BY Name
        ORDER BY avg_total desc 
        limit {TEAM_NUMBERS}
    )
    SELECT
        Name as TeamName,
        avg_player_points_per_game,
        avg_player_minutes_per_game,
        avg_player_assist_per_game,
        season
    FROM gold.team_basic_stats_agg
    WHERE Name in (SELECT TeamName FROM most_scoring_teams)
"""
)