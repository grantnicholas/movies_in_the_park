import pandas as pd
import numpy as np
import requests


def get_forecast(row, API_KEY):
	latitude = row["latitude"]
	longitude = row["longitude"]
	date = row["date"]
	showtime = row["showtime"]

	return _get_forecast(latitude, longitude, date, showtime, API_KEY)


def _parse_darksky_response(dictionary):
	current_data = dictionary["currently"]
	temperature = current_data["temperature"]
	humidity = current_data["humidity"]
	precip_prob = current_data["precipProbability"]
	return (temperature, humidity, precip_prob)

def _forecast_url(API_KEY, latitude, longitude, datetime):
	url = "https://api.darksky.net/forecast/{API_KEY}/{LAT},{LONG},{DATETIME}?exclude=hourly,daily,flags".format(
		API_KEY=API_KEY, LAT=latitude, LONG=longitude, DATETIME=datetime.strftime("%Y-%m-%dT%H:%M:%S")
		)
	return url

def _get_forecast(latitude, longitude, date, showtime, API_KEY):
	if pd.isnull(latitude) or pd.isnull(longitude):
		return (np.NaN, np.NaN, np.NaN, np.NaN)

	response = requests.get(
		_forecast_url(API_KEY, latitude, longitude, showtime)
	)
	if response.status_code == 404:
		last_year = showtime + pd.Timedelta(years=1)
		response = requests.get(
			_forecast_url(API_KEY, latitude, longitude, last_year)
		)
		response.raise_for_status()
		return (True,) + _parse_darksky_response(response.json())
	else:
		response.raise_for_status()
		return (False,) + _parse_darksky_response(response.json())
