import logging
from spark_session import create_spark_session
from pathlib import Path
from pyspark.sql.functions import (
    split, when, col, concat_ws, slice, cardinality, element_at, collect_list, when, lit, broadcast, monotonically_increasing_id
)

BASEDIR = Path(__file__).resolve().parent
SILVER_SCHEMA = "silver"
STATS = "stats"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

spark = create_spark_session()

spark.sql(f"CREATE schema IF NOT EXISTS {SILVER_SCHEMA}")

bronze_stats = spark.read.table("bronze.stats")
silver_dim_teams = spark.read.table("silver.dim_teams").withColumnRenamed("SK", "SKTeam")
silver_dim_salaries = spark.read.table("silver.dim_salary").withColumnRenamed("SK", "SKSalary")
silver_dim_awards = spark.read.table("silver.dim_awards").withColumnRenamed("SK", "SKAwards").withColumnRenamed("season", "season_award")
silver_dim_seasons = spark.read.table("silver.dim_seasons").withColumnRenamed("SK", "SKSeason").withColumnRenamed("season", "season_s")
silver_dim_injuries = spark.read.table("silver.dim_injuries").withColumnRenamed("SK", "SKInjuries")

stats_transformed = (
    bronze_stats.withColumn(
        "Name_Splitted", split("Name", " ")
    ).select(
        "*",
        element_at(col("Name_Splitted"), -1).alias("TeamAbbrev"),
        concat_ws(" ", slice(col("Name_Splitted"), 1, cardinality(col("Name_Splitted"))-1)).alias("PlayerName")
    )
    .withColumn("TeamAbbrev", when(col("TeamAbbrev").contains("/"), split("TeamAbbrev", "/").getItem(0)).otherwise(col("TeamAbbrev")))
)

silver_fact_stats = (
    stats_transformed
    .join(broadcast(silver_dim_teams), on=[stats_transformed.TeamAbbrev == silver_dim_teams.Abbreviation], how="left")
    .join(silver_dim_salaries, on=[
          stats_transformed.PlayerName == silver_dim_salaries.Player,
        stats_transformed.season.between(silver_dim_salaries.start_season, silver_dim_salaries.end_season)
    ], how="left"
    )
    .join(silver_dim_awards, on=[
          stats_transformed.PlayerName == silver_dim_awards.Player,
          stats_transformed.season == silver_dim_awards.season_award,
          stats_transformed.season_phase == 'Regular'
        ],
        how="left"
    )
    .select(
        col("RK"), 
        col("PlayerName"),
        col("SKTeam"),
        col("SKSalary"),
        col("SKAwards"),
        col("POS"), 
        col("GP"), 
        col("MIN"), 
        col("PTS"),
        col("FGM"), 
        col("FGA"), 
        col("FG%"), 
        col("3PM"), 
        col("3PA"), 
        col("3P%"),
        col("FTM"), 
        col("FTA"), 
        col("FT%"), 
        col("REB"), 
        col("AST"), 
        col("STL"),
        col("BLK"), 
        col("TO"), 
        col("DD2"), 
        col("TD3"), 
        col("season"), 
        col("season_phase")
    )
)

silver_fact_stats_columns = silver_fact_stats.columns
silver_fact_stats_columns.remove("SKAwards")

silver_fact_stats = silver_fact_stats.groupBy(
    *silver_fact_stats_columns
).agg(
    collect_list("SKAwards").alias("SKAwards")
).select(
    *silver_fact_stats_columns,
    when(cardinality("SKAwards") == 0, lit(None)).otherwise(col("SKAwards")).alias("SKAwards")
)

silver_fact_stats_with_seasons = (
    silver_fact_stats
    .join(broadcast(silver_dim_seasons), on=[
            stats_transformed.season == silver_dim_seasons.season_s
        ],
        how="left"
    )
)

silver_fact_stats_with_injuries = (
    silver_fact_stats_with_seasons.join(
        silver_dim_injuries,
        on=[
            silver_fact_stats_with_seasons.PlayerName == silver_dim_injuries.Player,
            silver_dim_injuries.injured_on.between(
                silver_fact_stats_with_seasons.season_start, silver_fact_stats_with_seasons.season_end
            ) | silver_dim_injuries.returned.between(
                silver_fact_stats_with_seasons.season_start, silver_fact_stats_with_seasons.season_end
            ),
            silver_fact_stats_with_seasons.season_phase == 'Regular'
        ],
        how="left"
    )
)

final_silver_stats = silver_fact_stats_with_injuries.select(
        col("RK"), 
        col("PlayerName"),
        col("SKTeam"),
        col("SKSalary"),
        col("SKAwards"),
        col("SKSeason"),
        col("SKInjuries"),
        col("POS"), 
        col("GP"), 
        col("MIN"), 
        col("PTS"),
        col("FGM"), 
        col("FGA"), 
        col("FG%"), 
        col("3PM"), 
        col("3PA"), 
        col("3P%"),
        col("FTM"), 
        col("FTA"), 
        col("FT%"), 
        col("REB"), 
        col("AST"), 
        col("STL"),
        col("BLK"), 
        col("TO"), 
        col("DD2"), 
        col("TD3"), 
        col("season"), 
        col("season_phase")
)

final_silver_stats_columns = final_silver_stats.columns
final_silver_stats_columns.remove("SKInjuries")

final_silver_stats_grouped = final_silver_stats.groupBy(
    *final_silver_stats_columns
).agg(
    collect_list("SKInjuries").alias("SKInjuries")
)

final_silver_stats_selected = final_silver_stats_grouped.select(
        monotonically_increasing_id().alias("SKStats"),
        col("PlayerName"),
        col("SKTeam"),
        col("SKSalary"),
        col("SKAwards"),
        col("SKSeason"),
        when(cardinality("SKInjuries") == 0, lit(None)).otherwise(col("SKInjuries")).alias("SKInjuries"),
        col("POS"), 
        col("GP"), 
        col("MIN"), 
        col("PTS"),
        col("FGM"), 
        col("FGA"), 
        col("FG%"), 
        col("3PM"), 
        col("3PA"), 
        col("3P%"),
        col("FTM"), 
        col("FTA"), 
        col("FT%"), 
        col("REB"), 
        col("AST"), 
        col("STL"),
        col("BLK"), 
        col("TO"), 
        col("DD2"), 
        col("TD3"), 
        col("season"), 
        col("season_phase")
)

if spark.catalog.tableExists(f"{SILVER_SCHEMA}.{STATS}"):
    logger.info("Table exists, adding partition")
    final_silver_stats_selected.writeTo(f"{SILVER_SCHEMA}.{STATS}").partitionedBy("season", "season_phase").overwritePartitions()
else:
    logger.info("Table doesn't exists, creating table")
    final_silver_stats_selected.writeTo(f"{SILVER_SCHEMA}.{STATS}").partitionedBy("season", "season_phase").createOrReplace()
  