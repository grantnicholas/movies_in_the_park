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
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import timedelta
import logging

from spothero_challenge.dataframe_processing import enrich_movies, enrich_movies_with_weather, enrich_titles_and_ratings
from spothero_challenge.workflow_utils import fetch_file, WorkflowDataFetcher


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

def fetch_movie_titles_task():
	did_change = fetcher.fetch(lambda write_path: fetch_file("https://datasets.imdbws.com/title.basics.tsv.gz", write_path), "title.basics.tsv.gz")
	logging.info("Fetched title.basics.tsz.gz; changed: {}".format(did_change))

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

	joined_df = movies_with_weather_df.merge(imdb_df, how='left', left_on='title', right_on='primaryTitle')
	# Snapshot this data out since it might be useful for debugging
	fetcher.fetch(lambda write_path: joined_df.to_parquet(write_path), "report.unfiltered.parquet.snappy")

	# Create the final report
	# Filter with criteria from spec
	filtered_df = joined_df[ (joined_df["temperature"] > 70) & (joined_df["temperature"] < 90) & (joined_df["humidity"] < .80) & (joined_df["precip_prob"] < .30) ]

	filtered_df["deviation_temperature"] = (filtered_df["temperature"] - 80).abs()
	filtered_df["deviation_humidity"] = (filtered_df["humidity"] - 80).abs()
	filtered_df["deviation_precip_prob"] = (filtered_df["precip_prob"] - 0).abs()

	deviation_cols = ["averageRating", "deviation_temperature", "deviation_humidity", "deviation_precip_prob"]

	# min-max normalization 
	# ensures each feature is on equal footing (scaled from [0,1])
	# otherwise you can't compare temperature in F to humidity in %
	normalized_deviations = (filtered_df[deviation_cols] - filtered_df[deviation_cols].min()) / (filtered_df[deviation_cols].max() - filtered_df[deviation_cols].min())

	filtered_df["normalized_average_ratings"], filtered_df["normalized_deviation_temperature"], filtered_df["normalized_deviation_humidity"], filtered_df["normalized_deviation_precip_prob"] = normalized_deviations["averageRating"], normalized_deviations["deviation_temperature"], normalized_deviations["deviation_humidity"], normalized_deviations["deviation_precip_prob"]

	# Do we want to consider normalized deviations in temperature, humidity, and precip to be the same?
	# For now lets do the simplest thing and say they are all equal, but we can tweak the weights if people don't like rain as much as temperature (for example)
	temperature_weight = 1
	humidity_weight = 1
	precip_weight = 1

	filtered_df["cost"] = filtered_df["normalized_deviation_temperature"]*temperature_weight + filtered_df["normalized_deviation_humidity"]*humidity_weight + filtered_df["normalized_deviation_precip_prob"] * precip_weight

	filtered_df.sort_values(["cost"], ascending=[True])

	filtered_df.to_csv(
		fetcher._get_data_path("report.final.csv"), index=False, encoding="utf-8"
	)



with DAG(dag_id='spothero_challenge', default_args=args, schedule_interval='@daily', dagrun_timeout=timedelta(minutes=60), max_active_runs=1) as dag:
	chicago_movies_task = PythonOperator(task_id="fetch_chicago_movies", python_callable=fetch_chicago_movies_task)
	ratings_task = PythonOperator(task_id="fetch_movie_ratings", python_callable=fetch_movie_ratings_task)
	movie_titles_task = PythonOperator(task_id="fetch_movie_titles", python_callable=fetch_movie_titles_task)
	movies_weather_task = PythonOperator(task_id="fetch_chicago_movies_weather", python_callable=fetch_chicago_movies_weather_task)
	enrich_imdb_task = PythonOperator(task_id="enrich_imdb_movie_data", python_callable=enrich_imdb_movie_data_task)
	join_imdb_to_weathermovies_task = PythonOperator(task_id="join_imdb_to_movies_with_weather", python_callable=join_imdb_to_movies_with_weather_task)

	chicago_movies_task >> movies_weather_task >> join_imdb_to_weathermovies_task

	[movie_titles_task, ratings_task] >> enrich_imdb_task >> join_imdb_to_weathermovies_task

# Main for manual testing	
def main():
	# fetch_chicago_movies_task()
	# fetch_chicago_movies_weather_task()

	# fetch_movie_ratings_task()
	# fetch_movie_titles_task()
	# enrich_imdb_movie_data_task()
	join_imdb_to_movies_with_weather_task()


if __name__ == "__main__":
	main()
