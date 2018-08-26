import requests
import pandas as pd
import json
import os
from datetime import timedelta

from spothero_challenge.dataframe_processing import (
    enrich_movies, enrich_movies_with_weather,
    enrich_titles_and_ratings, create_final_report, filter_only_unshown_movies
)
from spothero_challenge.workflow_utils import fetch_file, fetch_etag, WorkflowDataManager

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

data_manager = WorkflowDataManager(workflow_dir="/data")


def fetch_chicago_movies_task():
    response = requests.get("https://data.cityofchicago.org/resource/dan6-dh2g.json")
    ETAG = response.headers.get("ETag", None)
    data = response.json()
    # Save the json to disk, useful for debugging even though it adds a roundtrip
    with open(data_manager.get_data_path("movies.json"), "w+") as f:
        json.dump(data, f)

    def enrich():
        movies_df = pd.read_json(
            data_manager.get_data_path("movies.json")
        )
        enriched_movies = enrich_movies(movies_df)
        enriched_movies.to_parquet(
            data_manager.get_data_path("movies.parquet.snappy")
        )

    did_change = data_manager.update_cache_tag(
        "etag.movies.json",
        ETAG,
        did_change_func=enrich, did_not_change_func=enrich
    )
    return did_change


def maybe_skip_weather(**kwargs):
    ti = kwargs['ti']
    did_chicago_movies_change = ti.xcom_pull(task_ids='fetch_chicago_movies')
    does_weather_data_exist = os.path.exists(
        data_manager.get_data_path("movies_with_forecasts.parquet.snappy")
    )
    if did_chicago_movies_change or not does_weather_data_exist:
        return "fetch_chicago_movies_weather"
    else:
        return "skipped_weather_task"


def fetch_chicago_movies_weather_task():
    movies_df = pd.read_parquet(
        data_manager.get_data_path("movies.parquet.snappy")
    )
    API_KEY = os.environ["DARKSKY_API_KEY"]
    movies_df = enrich_movies_with_weather(movies_df, API_KEY)

    movies_df.to_parquet(
        data_manager.get_data_path("movies_with_forecasts.parquet.snappy")
    )


def fetch_movie_ratings_task():
    url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    ETAG = fetch_etag(url)

    did_change = data_manager.update_cache_tag(
        "etag.title.ratings.tsv.gz",
        ETAG,
        did_change_func=lambda: fetch_file(url, data_manager.get_data_path("title.ratings.tsv.gz"))
    )
    return did_change


def fetch_movie_titles_task():
    url = "https://datasets.imdbws.com/title.basics.tsv.gz"
    ETAG = fetch_etag(url)

    did_change = data_manager.update_cache_tag(
        "etag.title.basics.tsv.gz",
        ETAG,
        did_change_func=lambda: fetch_file(url, data_manager.get_data_path("title.basics.tsv.gz"))
    )
    return did_change


def enrich_imdb_movie_data_task():
    ratings_df = pd.read_csv(
        data_manager.get_data_path("title.ratings.tsv.gz"),
        sep='\t', compression='gzip', header=0
    )

    title_df = pd.read_csv(
        data_manager.get_data_path("title.basics.tsv.gz"),
        sep='\t', compression='gzip', header=0
    )

    out_df = enrich_titles_and_ratings(title_df, ratings_df)
    out_df.to_parquet(
        data_manager.get_data_path("imdb.joined.parquet.snappy")
    )


def join_imdb_to_movies_with_weather_task():
    movies_with_weather_df = pd.read_parquet(
        data_manager.get_data_path("movies_with_forecasts.parquet.snappy")
    )
    imdb_df = pd.read_parquet(
        data_manager.get_data_path("imdb.joined.parquet.snappy")
    )

    final_report_df = create_final_report(movies_with_weather_df, imdb_df)
    final_report_df.to_parquet(
        data_manager.get_data_path("report.unfiltered.parquet")
    )


def create_final_report_with_datefilter():
    report_df = pd.read_parquet(
        data_manager.get_data_path("report.unfiltered.parquet")
    )

    final_report_df = filter_only_unshown_movies(report_df)

    final_report_df.to_csv(
        data_manager.get_data_path("report.final.csv"), index=False, encoding="utf-8"
    )


# every 30 minutes
schedule_interval = '*/30 * * * *'
with DAG(dag_id='spothero_challenge', default_args=args, schedule_interval=schedule_interval,
         dagrun_timeout=timedelta(minutes=25), max_active_runs=1, catchup=False) as dag:
    chicago_movies_task = PythonOperator(task_id="fetch_chicago_movies", python_callable=fetch_chicago_movies_task)
    maybe_skip_weather_task = BranchPythonOperator(task_id="maybe_skip_weather_task",
                                                   python_callable=maybe_skip_weather, provide_context=True)
    movies_weather_task = PythonOperator(task_id="fetch_chicago_movies_weather",
                                         python_callable=fetch_chicago_movies_weather_task)
    ratings_task = PythonOperator(task_id="fetch_movie_ratings", python_callable=fetch_movie_ratings_task)
    movie_titles_task = PythonOperator(task_id="fetch_movie_titles", python_callable=fetch_movie_titles_task)
    enrich_imdb_task = PythonOperator(task_id="enrich_imdb_movie_data", python_callable=enrich_imdb_movie_data_task)
    join_imdb_to_weathermovies_task = PythonOperator(task_id="join_imdb_to_movies_with_weather",
                                                     python_callable=join_imdb_to_movies_with_weather_task)
    final_report_task = PythonOperator(task_id="final_report", python_callable=create_final_report_with_datefilter)

    # Chicago movies in the park with weather flow
    (
            chicago_movies_task >>
            maybe_skip_weather_task >>
            [movies_weather_task, DummyOperator(task_id="skipped_weather_task")] >>
            DummyOperator(task_id="join_skipped_tasks", trigger_rule='one_success') >>
            join_imdb_to_weathermovies_task
    )

    # IMDB movie data flow
    [movie_titles_task, ratings_task] >> enrich_imdb_task >> join_imdb_to_weathermovies_task

    # Joined flow
    join_imdb_to_weathermovies_task >> final_report_task
