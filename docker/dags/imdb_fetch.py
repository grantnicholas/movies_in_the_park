import pandas as pd
import numpy as np


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

def enrich_movies(movies_df):
	movies_df["longitude"], movies_df["latitude"] = zip(*movies_df["location"].apply(_extract_lat_and_long))
	return movies_df.drop(columns=["location"])