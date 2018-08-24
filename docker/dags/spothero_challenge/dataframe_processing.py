import pandas as pd
import numpy as np
from spothero_challenge.darksky_api import get_forecast


# Movie processing
def enrich_movies(movies_df):
	movies_df["longitude"], movies_df["latitude"] = zip(*movies_df["location"].apply(_extract_lat_and_long))
	return movies_df.drop(columns=["location"])


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

def enrich_movies_with_weather(movies_df, DARKSKY_API_KEY):
	movies_df["showtime"] = movies_df["date"] + pd.Timedelta(hours=8)

	movies_df["forecast_current_year"], movies_df["temperature"], movies_df["humidity"], movies_df["precip_prob"]  = zip(
		*movies_df.apply(lambda row: get_forecast(row, API_KEY), axis=1)
	)

	return movies_df

# Title processing
def enrich_titles_and_ratings(titles_df, ratings_df):
	transformed_titles = _transform_titles(titles_df)
	joined = _join_titles_and_ratings(transformed_titles, ratings_df)
	out_df = _remove_dupe_movies_by_title(joined)
	return out_df

def _transform_titles(titles_df):
	movie_titles_df = titles_df[titles_df["titleType"] == "movie"]
	not_useful_columns = ["isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
	return movie_titles_df.drop(columns=not_useful_columns)

def _join_titles_and_ratings(titles_df, ratings_df):
	joined = titles_df.merge(ratings_df, on='tconst')
	return joined


def _remove_dupe_movies_by_title(df):
	# Multiple movies with the same title exist
	# We don't know which movies Chicago in the park will play since we are only given a title
	# A good first guess might be choosing the "most well known" movies (ie: the movie with the highest number of votes in the group)
	deduped = df.sort_values("numVotes", ascending=False).groupby(by=["primaryTitle"]).first()
	deduped.reset_index(level=0, inplace=True)
	return deduped