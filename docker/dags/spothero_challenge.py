import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import timedelta


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

def fetch_chicago_movies_task():
	movies_df = fetch_movies()
	enriched_movies_df = enrich_movies(movies_df)
	enriched_movies_df.to_parquet("/tmp/movies.parquet.snappy")

def fetch_movie_ratings_task():
	ratings_df = fetch_ratings()
	titles_df = fetch_titles()
	overall_ratings_df = join_titles_and_ratings(titles_df, ratings_df)
	overall_ratings_df.to_parquet("/tmp/ratings.parquet.snappy")





with DAG(dag_id='spothero_challenge', default_args=args, schedule_interval='@daily', dagrun_timeout=timedelta(minutes=60) ) as dag:
	(
		PythonOperator(task_id="fetch_chicago_movies", python_callable=fetch_chicago_movies_task) 
		>> PythonOperator(task_id="second", python_callable=fetch_movie_ratings_task)
	)


# DAG definition above, library code below
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


def _extract_lat_and_long(location_dict):
	# Not documented in API docs, but some locations are missing
	if pd.isnull(location_dict):
		return (np.NaN, np.NaN)
	else:
		coordinates = location_dict.get("coordinates", None)
		if coordinates is None:
			return (np.NaN, np.NaN)
		else:		
			# From the API docs, nonstandard order of coordinates
			(longitude, latitude) = coordinates
			return (longitude, latitude)

def fetch_movies():
	response = requests.get("https://data.cityofchicago.org/resource/dan6-dh2g.json")
	data = response.json()
	return pd.DataFrame.from_records(data)

def enrich_movies(movies_df):
	movies_df["longitude"], movies_df["latitude"] = zip(*movies_df["location"].apply(_extract_lat_and_long))
	return movies_df.drop(columns=["location"])


def _fetch_file_as_bytes(url):
	response = requests.get(url)
	byte_file_like = BytesIO()
	for l in response.iter_content():
		byte_file_like.write(l)
	byte_file_like.seek(0)
	return byte_file_like

def _convert_gzipped_file_to_file(f):
	gzip_file_like = gzip.GzipFile(fileobj=f, mode='rb')
	string_file_like = StringIO()
	for l in gzip_file_like:
		string_file_like.write(l.decode('UTF-8'))
	string_file_like.seek(0)
	return string_file_like

def fetch_gzipped_file(url):
	"""
	Fetches a gzipped file from a url, returning a file-like object you can work with
	Note: the current implementation assumes the file will fit in memory

	url: String representing the file url to download
	returns: StringIO object representing decoded file data 
	"""
	byte_file_like = _fetch_file_as_bytes(url)
	return _convert_gzipped_file_to_file(byte_file_like)


def fetch_ratings():
	file_like = fetch_gzipped_file("https://datasets.imdbws.com/title.ratings.tsv.gz")
	df = pd.DataFrame.from_csv(file_like, sep='\t', header=0)
	return df 


def transform_titles(titles_df):
	movie_titles_df = titles_df[titles_df["titleType"] == "movie"]
	not_useful_columns = ["isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
	return movie_titles_df.drop(columns=not_useful_columns)

def fetch_titles():
	file_like = fetch_gzipped_file("https://datasets.imdbws.com/title.basics.tsv.gz")
	df = pd.DataFrame.from_csv(file_like, sep='\t', header=0)
	return transform_titles(df)


def join_titles_and_ratings(titles_df, ratings_df):
	joined = titles_df.join(ratings_df)
	return joined


def remove_dupe_movies_by_title():
	# Multiple movies with the same title exist
	# We don't know which movies Chicago in the park will play since we are only given a title
	# A good first guess might be choosing the "most well known" movies (ie: the movie with the highest number of votes in the group)
	deduped = df.sort_values("numVotes", ascending=False).groupby(by=["primaryTitle"]).first()
	deduped.reset_index(level=0, inplace=True)
	return deduped


def main():
	# fetch_chicago_movies_task()
	fetch_movie_ratings_task()

	# ratings_df = fetch_ratings()
	# print(ratings_df)

	# titles_df = fetch_titles()
	# print(titles_df)

if __name__ == "__main__":
	main()
