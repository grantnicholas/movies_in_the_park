import requests
import pandas as pd
import numpy as np
import gzip
import csv
import json
import os
import shutil
from uuid import uuid4
import hashlib
from datetime import timedelta
import logging
import csv

from spothero_challenge.dataframe_processing import enrich_movies, enrich_movies_with_weather, enrich_titles_and_ratings, create_final_report
from spothero_challenge.workflow_utils import fetch_file, WorkflowDataFetcher


import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

fetcher = WorkflowDataFetcher(workflow_dir = "/tmp/spothero_challenge")


def fetch_chicago_movies_task():
	def _fetch_movies(write_path):
		response = requests.get("https://data.cityofchicago.org/resource/dan6-dh2g.json")
		data = response.json()
		with open(write_path, "w+") as f:
			json.dump(data, f)

	did_change = fetcher.fetch(_fetch_movies, "movies.json")
	logging.info("Fetched movies.json; changed: {}".format(did_change))

	movies_df = pd.read_json(
		fetcher._get_data_path("movies.json")
	)
	enriched_movies = enrich_movies(movies_df)
	enriched_movies.to_parquet(
		fetcher._get_data_path("movies.parquet.snappy")
	)
	return did_change

def maybe_skip_weather(**kwargs):
	ti = kwargs['ti']
	did_chicago_movies_change = ti.xcom_pull(task_ids='fetch_chicago_movies')
	if did_chicago_movies_change:
		return "fetch_chicago_movies_weather"
	else:
		return "skipped_weather_task"


def fetch_chicago_movies_weather_task():
	movies_df = pd.read_parquet(
		fetcher._get_data_path("movies.parquet.snappy")
	)
	API_KEY  = os.environ["DARKSKY_API_KEY"]
	movies_df = enrich_movies_with_weather(movies_df, API_KEY)


	movies_df.to_parquet(
		fetcher._get_data_path("movies_with_forecasts.parquet.snappy")
	)

def fetch_movie_ratings_task():
	did_change = fetcher.fetch(lambda write_path: fetch_file("https://datasets.imdbws.com/title.ratings.tsv.gz", write_path), "title.ratings.tsv.gz")
	logging.info("Fetched title.ratings.tsv.gz; changed: {}".format(did_change))
	return did_change

def fetch_movie_titles_task():
	did_change = fetcher.fetch(lambda write_path: fetch_file("https://datasets.imdbws.com/title.basics.tsv.gz", write_path), "title.basics.tsv.gz")
	logging.info("Fetched title.basics.tsz.gz; changed: {}".format(did_change))
	return did_change

def enrich_imdb_movie_data_task():
	ratings_df = pd.read_csv(
		fetcher._get_data_path("title.ratings.tsv.gz"),
		sep='\t', compression='gzip', header=0
	)	

	title_df = pd.read_csv(
		fetcher._get_data_path("title.basics.tsv.gz"), 
		sep='\t', compression='gzip', header=0
	)	

	out_df = enrich_titles_and_ratings(title_df, ratings_df)
	out_df.to_parquet(
		fetcher._get_data_path("imdb.joined.parquet.snappy")
	)

def join_imdb_to_movies_with_weather_task():
	movies_with_weather_df = pd.read_parquet(
		fetcher._get_data_path("movies_with_forecasts.parquet.snappy")
	)
	imdb_df = pd.read_parquet(
		fetcher._get_data_path("imdb.joined.parquet.snappy")
	)

	final_report_df = create_final_report(movies_with_weather_df, imdb_df)
	final_report_df.to_csv(
		fetcher._get_data_path("report.final.csv"), index=False, encoding="utf-8"
	)



with DAG(dag_id='spothero_challenge', default_args=args, schedule_interval='@daily', dagrun_timeout=timedelta(minutes=60), max_active_runs=1) as dag:
	chicago_movies_task = PythonOperator(task_id="fetch_chicago_movies", python_callable=fetch_chicago_movies_task)
	maybe_skip_weather_task = BranchPythonOperator(task_id="maybe_skip_weather_task", python_callable=maybe_skip_weather, provide_context=True)
	movies_weather_task = PythonOperator(task_id="fetch_chicago_movies_weather", python_callable=fetch_chicago_movies_weather_task)
	ratings_task = PythonOperator(task_id="fetch_movie_ratings", python_callable=fetch_movie_ratings_task)
	movie_titles_task = PythonOperator(task_id="fetch_movie_titles", python_callable=fetch_movie_titles_task)
	enrich_imdb_task = PythonOperator(task_id="enrich_imdb_movie_data", python_callable=enrich_imdb_movie_data_task)
	join_imdb_to_weathermovies_task = PythonOperator(task_id="join_imdb_to_movies_with_weather", python_callable=join_imdb_to_movies_with_weather_task)

	(
	 chicago_movies_task >> 
	 maybe_skip_weather_task >> [movies_weather_task, DummyOperator(task_id="skipped_weather_task")] >> DummyOperator(task_id="join_skipped_tasks", trigger_rule='one_success') >>
	 join_imdb_to_weathermovies_task
	)

	[movie_titles_task, ratings_task] >> enrich_imdb_task >> join_imdb_to_weathermovies_task

# Main for manual testing	
def main():
	# Override the workflow dir to store data in the data directory
	# This is nice for manual testing, the data pulled for manual testing gets mounted into the airflow container
	directory = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
	data_dir = os.path.join(directory, "data")
	fetcher._workflow_dir = data_dir
	
	fetch_chicago_movies_task()
	# fetch_chicago_movies_weather_task()

	fetch_movie_ratings_task()
	fetch_movie_titles_task()
	enrich_imdb_movie_data_task()
	# join_imdb_to_movies_with_weather_task()




if __name__ == "__main__":
	main()
