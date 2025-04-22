from spark_session import create_spark_session

GOLD_SCHEMA = "gold"
MVP_STATS = "mvp_stats"

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {GOLD_SCHEMA}")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {GOLD_SCHEMA}.{MVP_STATS} AS
    with exploaded as (
    SELECT
        SKStats,
        PlayerName,
        explode(SKAwards) as SKAwards,
        POS as position,
        GP as games_played,
        PTS as points,
        MIN as minutes_per_game,
        AST as asist_per_game,
        DD2 as double_double,
        TD3 as triple_double,
        REB as rebounds_per_game,
        season,
        season_phase
    FROM silver.stats
    )
    SELECT 
        e.PlayerName,
        e.position,
        e.games_played,
        e.points,
        e.minutes_per_game,
        e.asist_per_game,
        e.double_double,
        e.triple_double,
        e.rebounds_per_game,
        e.season,
        a.Award
    FROM exploaded e
    JOIN silver.dim_awards a ON e.SKAwards = a.SK and a.Award = 'MVP'
    ORDER BY season DESC
"""
)