from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
AGG_BASIC_TEAM_STATS = "avg_days_missed_by_team_player"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{AGG_BASIC_TEAM_STATS} AS
    with exploded as (
        SELECT
            SKStats,
            PlayerName,
            SKTeam,
            GP as games_played,
            season,
            season_phase,
            explode(SKInjuries) as SKInjuries
        FROM silver.stats
    ),
    days_missed as (
        SELECT 
            SKStats,
            PlayerName,
            SKTeam,
            games_played,
            season,
            season_phase,
            CAST(SUM(days_missed) as INT) as days_missed_injury
        FROM exploded e
        JOIN silver.dim_injuries di ON e.SKInjuries = di.SK
        GROUP BY SKStats, PlayerName, SKTeam, games_played, season ,season_phase
    ),
    agg_days_missed_by_injury (
    SELECT
            s.SKTeam,
            s.season,
            SUM(CASE WHEN dm.days_missed_injury is null then 0 else dm.days_missed_injury END) / COUNT(DISTINCT s.PlayerName) as avg_days_missed_by_player
        FROM silver.stats s
        LEFT JOIN days_missed dm ON s.SKStats = dm.SKStats
        GROUP BY s.SKTeam, s.season 
    )
    SELECT
        t.Name as TeamName,
        agg.SKTeam,
        agg.season,
        ROUND(agg.avg_days_missed_by_player, 1) as avg_days_missed_by_player,
        ds.season_length_days
    FROM agg_days_missed_by_injury agg
    JOIN silver.dim_seasons ds ON agg.season = ds.season
    JOIN silver.dim_teams t ON agg.SKTeam = t.SK
    ORDER BY avg_days_missed_by_player desc
"""
)