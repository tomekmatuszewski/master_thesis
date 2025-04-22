from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
AGG_BASIC_TEAM_STATS = "team_basic_stats_agg"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{AGG_BASIC_TEAM_STATS} AS
    with agg_stats as (
        SELECT
            SKTeam,
            AVG(PTS) as avg_player_points_per_game,
            AVG(MIN) as avg_player_minutes_per_game,
            AVG(AST) as avg_player_assist_per_game,
            season
        FROM silver.stats
        GROUP BY SKTeam, season
    )
    SELECT
        dim_teams.Name,
        ROUND(avg_player_points_per_game, 2) as avg_player_points_per_game,
        ROUND(avg_player_minutes_per_game, 2) as avg_player_minutes_per_game,
        ROUND(avg_player_assist_per_game, 2) as avg_player_assist_per_game,
        season
    FROM agg_stats
    JOIN silver.dim_teams dim_teams
    ON agg_stats.SKTeam = dim_teams.SK
    ORDER BY avg_player_points_per_game desc, season desc
"""
)