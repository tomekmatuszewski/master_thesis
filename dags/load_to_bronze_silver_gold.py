from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Connection
from airflow.settings import Session
import os

args = {
    'owner': 'admin',
    'start_date': days_ago(1)
}

jar_path = "/home/iceberg/jobs/jars/"
jars = ",".join([f"{jar_path}{file}" for file in os.listdir(jar_path)])

conf = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.demo": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.demo.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.demo.warehouse": "s3://lakehouse/",
    "spark.sql.catalog.demo.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.demo.uri": "http://rest:8181",
    "spark.sql.defaultCatalog": "demo",
    "spark.sql.catalogImplementation": "in-memory",
    "spark.sql.catalog.demo.type": "rest",
    "spark.executor.heartbeatInterval": "300000",
    "spark.network.timeout": "400000"
}


with DAG(
    'spark_load_to_bronze_silver_gold', 
    schedule_interval=None, 
    default_args=args,
    concurrency=1,
    catchup=False
    ):

    @task
    def deploy_spark_connections():
        session = Session()
        conn_spark = Connection(
            conn_id='spark_default',
            conn_type='Spark',
            description='connection to spark container',
            host='spark://spark-iceberg-pm',
            port=7077,
            extra={"queue": "root.default"}
        )
        conn = session.query(Connection).filter(Connection.conn_id == "spark_default").one()
        if conn:
            session.query(Connection).filter(Connection.conn_id == "spark_default").delete()
            print("Deleted existing connection: spark_default")
        session.add(conn_spark)
        session.commit()
        print("Connection spark-default created!")

    start = EmptyOperator(
        task_id="start"
    )

    # stats_to_bronze = SparkSubmitOperator(
    #     task_id="spark_job_stats",
    #     application='/home/iceberg/jobs/bronze_stats.py',
    #     name="load_stats_tables",
    #     conn_id="spark_default",
    #     verbose=True,
    #     jars=jars,
    #     conf=conf,
    # )

    stats_to_bronze = EmptyOperator(task_id="stats_to_bronze")

    injuries_to_bronze = SparkSubmitOperator(
        task_id="injuries_to_bronze",
        application='/home/iceberg/jobs/bronze_injuries.py',
        name="load_injuries_table",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )
    # injuries_to_bronze = EmptyOperator(task_id="injuries_to_bronze")

    # salaries_to_bronze = SparkSubmitOperator(
    #     task_id="salaries_to_bronze",
    #     application='/home/iceberg/jobs/bronze_salaries.py',
    #     name="load_salaries_table",
    #     conn_id="spark_default",
    #     driver_class_path=jars,
    #     jars=jars,
    #     conf=conf,
    #     verbose=True
    # )
    salaries_to_bronze = EmptyOperator(task_id="salaries_to_bronze")

    awards_to_bronze = SparkSubmitOperator(
        task_id="awards_to_bronze",
        application='/home/iceberg/jobs/bronze_awards.py',
        name="load_awards_table",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    teams_to_bronze = SparkSubmitOperator(
        task_id="teams_to_bronze",
        application='/home/iceberg/jobs/bronze_teams.py',
        name="teams_to_bronze",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    seasons_to_bronze = SparkSubmitOperator(
        task_id="seasons_to_bronze",
        application='/home/iceberg/jobs/bronze_seasons.py',
        name="seasons_to_bronze",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    dim_teams_to_silver = SparkSubmitOperator(
        task_id="dim_teams_to_silver",
        application='/home/iceberg/jobs/silver_dim_teams.py',
        name="dim_teams_to_silver",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    dim_seasons_to_silver = SparkSubmitOperator(
        task_id="dim_seasons_to_silver",
        application='/home/iceberg/jobs/silver_dim_seasons.py',
        name="dim_seasons_to_silver",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    dim_salary_to_silver = SparkSubmitOperator(
        task_id="dim_salary_to_silver",
        application='/home/iceberg/jobs/silver_dim_salary.py',
        name="dim_salary_to_silver",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    dim_awards_to_silver = SparkSubmitOperator(
        task_id="dim_awards_to_silver",
        application='/home/iceberg/jobs/silver_dim_awards.py',
        name="dim_awards_to_silver",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    dim_injuries_to_silver = SparkSubmitOperator(
        task_id="dim_injuries_to_silver",
        application='/home/iceberg/jobs/silver_dim_injuries.py',
        name="dim_injuries_to_silver",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    stats_to_silver = SparkSubmitOperator(
        task_id="stats_silver",
        application='/home/iceberg/jobs/silver_stats.py',
        name="silver_stats",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_team_basic_stats = SparkSubmitOperator(
        task_id="gold_team_basic_stats",
        application='/home/iceberg/jobs/gold_team_basic_stats_agg.py',
        name="gold_team_basic_stats",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_avg_days_missed_by_player_to_gold = SparkSubmitOperator(
        task_id="gold_avg_days_missed_by_player_gold",
        application='/home/iceberg/jobs/gold_avg_days_missed_by_player.py',
        name="gold_avg_days_missed_by_player",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_mvp_stats = SparkSubmitOperator(
        task_id="gold_mvp_stats",
        application='/home/iceberg/jobs/gold_mvp_stats.py',
        name="gold_mvp_stats",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_salaries_stats = SparkSubmitOperator(
        task_id="gold_salaries_stats",
        application='/home/iceberg/jobs/gold_salaries_stats.py',
        name="gold_salaries_stats",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_salaries_runaway = SparkSubmitOperator(
        task_id="gold_salaries_runaway",
        application='/home/iceberg/jobs/gold_salaries_runaway.py',
        name="gold_salaries_runaway",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_last_five_season_most_injured_teams = SparkSubmitOperator(
        task_id="gold_last_five_season_most_injured_teams",
        application='/home/iceberg/jobs/gold_last_five_season_most_injured_teams.py',
        name="gold_last_five_season_most_injured_teams",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    gold_most_scoring_teams = SparkSubmitOperator(
        task_id="gold_most_scoring_teams",
        application='/home/iceberg/jobs/gold_most_scoring_teams.py',
        name="gold_most_scoring_teams",
        conn_id="spark_default",
        driver_class_path=jars,
        jars=jars,
        conf=conf,
        verbose=True
    )

    stop = EmptyOperator(
        task_id="stop"
    )

    # start >> [stats_to_bronze, salaries_to_bronze, injuries_to_bronze, teams_to_bronze] >> stop

    deploy_spark_connections() >> start
    start  >> teams_to_bronze >> dim_teams_to_silver >> stop
    start >> [stats_to_bronze, dim_teams_to_silver, dim_salary_to_silver, dim_awards_to_silver, dim_seasons_to_silver, dim_injuries_to_silver] >> stats_to_silver  >> stop
    start >> salaries_to_bronze  >> dim_salary_to_silver >> stop
    start >> injuries_to_bronze >> dim_injuries_to_silver >> stop
    start >> awards_to_bronze >> dim_awards_to_silver >> stop
    start >> seasons_to_bronze >> dim_seasons_to_silver >> stop
    stats_to_silver >> [gold_team_basic_stats, gold_avg_days_missed_by_player_to_gold, gold_mvp_stats, gold_salaries_stats] >> stop
    gold_salaries_stats >> gold_salaries_runaway >> stop
    gold_avg_days_missed_by_player_to_gold >> gold_last_five_season_most_injured_teams >> stop
    gold_team_basic_stats >> gold_most_scoring_teams >> stop