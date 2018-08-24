import pytest
from ..dags import spothero_challenge, darksky_api
import datetime

def test():
	assert 1==1


def test_construct_darksky_url():
	API_KEY = "XXX"
	latitude = 41.857935
	longitude = -87.622279
	date = datetime.datetime(year=2018, month=10, day=31, hour=0, minute=0)

	url = darksky_api._forecast_url(API_KEY, latitude, longitude, date)
	assert url == "https://api.darksky.net/forecast/XXX/41.857935,-87.622279,2018-10-31T00:00:00?exclude=hourly,daily,flags"


def test_parse_darkspy_api_response():
	sample_response = '{"latitude":41.857935,"longitude":-87.622279,"timezone":"America/Chicago","currently":{"time":1532869200,"summary":"Mostly Cloudy","icon":"partly-cloudy-day","precipIntensity":0,"precipProbability":0,"temperature":68.62,"apparentTemperature":68.66,"dewPoint":60.01,"humidity":0.74,"pressure":1019.4,"windSpeed":0.76,"windGust":1.73,"windBearing":269,"cloudCover":0.61,"uvIndex":1,"visibility":10,"ozone":337.27},"offset":-5}'

	out = darksky_api._parse_darksky_response(sample_response)

	assert out == (68.62, 0.74, 0)
