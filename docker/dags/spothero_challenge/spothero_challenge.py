# Library code here, DAG definition below
# In a production app we would not put everything in a single file but for a POC this makes deployment easy since the DAG is self contained
# In production either
# 1. Install the library code as a python library and import the library inside the DAG definition
# 2. Use airflow's packaged (zipped) DAG feature to package the library code with the DAG definition 

import requests
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import gzip
import csv
import json
import os
import shutil
from uuid import uuid4
import hashlib

from spothero_challenge.dataframe_processing import enrich_movies, enrich_movies_with_weather, enrich_titles_and_ratings
from spothero_challenge.workflow_utils import fetch_file, WorkflowDataFetcher



def _fetch_movies(write_path):
	response = requests.get("https://data.cityofchicago.org/resource/dan6-dh2g.json")
	data = response.json()
	with open(write_path, "w+") as f:
		json.dump(data, f)


# DAG definition

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import timedelta
import logging


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

fetcher = WorkflowDataFetcher(workflow_dir = "/tmp/spothero_challenge")


def fetch_chicago_movies_task():
	did_change = fetcher.fetch(_fetch_movies, "movies.json")
	logging.info("Fetched movies.json; changed: {}".format(did_change))

	movies_df = pd.read_json(
		fetcher._get_data_path("movies.json")
	)
	enriched_movies = enrich_movies(movies_df)
	enriched_movies.to_parquet(
		fetcher._get_data_path("movies.parquet.snappy")
	)


def parse_darksky_response(string):
	dictionary = json.loads(string)
	current_data = dictionary["currently"]
	temperature = current_data["temperature"]
	humidity = current_data["humidity"]
	precip_prob = current_data["precipProbability"]
	return (temperature, humidity, precip_prob)

def fetch_chicago_movies_weather_task():
	movies_df = pd.read_parquet(
		fetcher._get_data_path("movies.parquet.snappy")
	)
	API_KEY  = os.environ.get("DARKSKY_API_KEY", "f0c52be3fb6347fb5e97af2b78090956")
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



with DAG(dag_id='spothero_challenge', default_args=args, schedule_interval='@daily', dagrun_timeout=timedelta(minutes=60), max_active_runs=1) as dag:
	chicago_movies_task = PythonOperator(task_id="fetch_chicago_movies", python_callable=fetch_chicago_movies_task)
	ratings_task = PythonOperator(task_id="fetch_movie_ratings", python_callable=fetch_movie_ratings_task)
	movie_titles_task = PythonOperator(task_id="fetch_movie_titles", python_callable=fetch_movie_titles_task)
	movies_weather_task = PythonOperator(task_id="fetch_chicago_movies_weather", python_callable=fetch_chicago_movies_weather_task)
	enrich_imdb_task = PythonOperator(task_id="enrich_imdb_movie_data", python_callable=enrich_imdb_movie_data_task)

	chicago_movies_task >> movies_weather_task

	[movie_titles_task, ratings_task] >> enrich_imdb_task

# Main for testing	

def main():
	fetch_chicago_movies_task()
	# fetch_chicago_movies_weather_task()

	fetch_movie_ratings_task()
	fetch_movie_titles_task()
	enrich_imdb_movie_data_task()


if __name__ == "__main__":
	main()
