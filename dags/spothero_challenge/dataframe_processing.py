import pandas as pd
import numpy as np
from datetime import datetime, date
import pytz
from spothero_challenge.darksky_api import get_forecast


# Movie processing
def enrich_movies(movies_df):
    keep_these_cols = [c for c in list(movies_df.columns) if not c.startswith(":@computed_region")]
    filtered_movies_df = movies_df[keep_these_cols]

    filtered_movies_df["longitude"], filtered_movies_df["latitude"] = zip(
        *filtered_movies_df["location"].apply(_extract_lat_and_long)
    )
    return filtered_movies_df.drop(columns=["location"])


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
    # From the chicago movies docs, movies start at 8pm (20 hours into the day)
    movies_df["showtime"] = movies_df["date"] + pd.Timedelta(hours=20)

    movies_df["is_forecast_current_year"], movies_df["temperature"], movies_df["humidity"], movies_df["precip_prob"] = zip(
        *movies_df.apply(lambda row: get_forecast(row, DARKSKY_API_KEY), axis=1)
    )

    return movies_df


# Title processing
def enrich_titles_and_ratings(titles_df, ratings_df):
    transformed_titles = _transform_titles(titles_df)
    joined = _join_titles_and_ratings(transformed_titles, ratings_df)
    return joined


def _transform_titles(titles_df):
    movie_titles_df = titles_df[titles_df["titleType"] == "movie"]
    not_useful_columns = ["isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
    return movie_titles_df.drop(columns=not_useful_columns)


def _join_titles_and_ratings(titles_df, ratings_df):
    joined = titles_df.merge(ratings_df, on='tconst', how='inner')
    return joined


def _remove_dupe_movies_by_title(df, title_type):
    # Multiple movies with the same title exist
    # We don't know which movies Chicago in the park will play since we are only given a title
    # A good first guess might be choosing the "most well known" movies
    # (ie: the movie with the highest number of votes in the group)

    allowed_titles = ["primaryTitle", "originalTitle"]
    if title_type not in allowed_titles:
        raise Exception("Unknown title_type: {}; Must be one of {}".format(title_type, allowed_titles))

    deduped = df.sort_values("numVotes", ascending=False).groupby(by=[title_type]).first()
    return deduped.reset_index(level=0)


def _join_movies_with_weather_to_imdb(movies_with_weather_df, imdb_df):
    # Left join the movie data with weather to the imdb data
    # Try matching on primaryTitle first
    removed_dupes_primary_title_df = _remove_dupe_movies_by_title(imdb_df, "primaryTitle")
    joined_primary_title_df = movies_with_weather_df.merge(
        removed_dupes_primary_title_df, how='left', left_on='title', right_on='primaryTitle', validate='many_to_one'
    )

    # Partition the dataframe in two; one where we could find matches from the join and one where we could not
    matched_primary_title_df = joined_primary_title_df[~pd.isnull(joined_primary_title_df["primaryTitle"])]
    not_matched_primary_title_df = joined_primary_title_df[pd.isnull(joined_primary_title_df["primaryTitle"])]

    # Try matching on originalTitle this time
    removed_dupes_original_title_df = _remove_dupe_movies_by_title(imdb_df, "originalTitle")
    joined_original_title_df = not_matched_primary_title_df.drop(columns=imdb_df.columns).merge(
        removed_dupes_original_title_df, how='left', left_on='title', right_on='originalTitle', validate='many_to_one'
    )

    # Combine the primary_title matches and the original_title matches
    joined_df = pd.concat([matched_primary_title_df, joined_original_title_df])

    return joined_df


def _min_max_normalize(df, columns_to_normalize):
    # min-max normalization
    # ensures each feature is on equal footing (scaled from [0,1])
    # otherwise eg) you can't compare temperature in F to humidity in %
    normalized_deviations = (df[columns_to_normalize] - df[columns_to_normalize].min()) / (df[columns_to_normalize].max() - df[columns_to_normalize].min())

    for col in columns_to_normalize:
        normalized_col = "normalized_{}".format(col)
        df[normalized_col] = normalized_deviations[col]

    # mutates the input df
    return None


def _score_cost_function(df):
    # Do we want to consider normalized deviations in movie rating. temperature, humidity, and precip to be the same?
    # The simplest thing to do is to keep all the weights the same (so all factors are equal)
    # But some people might think rain ruins a movie night more than a bad movie does (for example)
    rating_weight = 1
    precip_weight = 1
    temperature_weight = 1
    humidity_weight = 1

    df["cost"] = df["normalized_deviation_average_rating"] * rating_weight + \
                 df["normalized_deviation_temperature"] * temperature_weight + \
                 df["normalized_deviation_humidity"] * humidity_weight + \
                 df["normalized_deviation_precip_prob"] * precip_weight


# Final report processing
def create_final_report(movies_with_weather_df, imdb_df):
    joined_df = _join_movies_with_weather_to_imdb(movies_with_weather_df, imdb_df)

    # Create the final report
    # Filter with criteria from spec about ideal temps
    filtered_df = joined_df[
        (joined_df["temperature"] > 70) & (joined_df["temperature"] < 90) & (joined_df["humidity"] < .80) & (
                    joined_df["precip_prob"] < .30)]

    # Ideal score of 10 (max) for an IMDB rating
    filtered_df["deviation_average_rating"] = (filtered_df["averageRating"] - 10).abs()
    # Ideal temp of 80 for a summer movie
    filtered_df["deviation_temperature"] = (filtered_df["temperature"] - 80).abs()
    # Ideal humidity of 73% for a summer movie
    filtered_df["deviation_humidity"] = (filtered_df["humidity"] - .73).abs()
    # Ideal 0% chance of rain for a summer movie
    filtered_df["deviation_precip_prob"] = (filtered_df["precip_prob"] - 0).abs()

    deviation_cols = ["deviation_average_rating", "deviation_temperature", "deviation_humidity", "deviation_precip_prob"]
    _min_max_normalize(filtered_df, columns_to_normalize=deviation_cols)
    _score_cost_function(filtered_df)

    # Only keep columns in the report people care about
    main_cols = ["date", "day", "park", "park_address", "title", "averageRating", "is_forecast_current_year",
                 "temperature", "precip_prob", "humidity", "cost"]
    extra_cols = ["rating", "underwriter", "park_phone"]
    output_df = filtered_df[main_cols + extra_cols]

    output_df.rename(columns={"averageRating": "average_imdb_rating", "rating": "mpaa_rating"}, inplace=True)
    output_df.sort_values(["cost"], ascending=[True], inplace=True)
    return output_df


def _get_current_datetime_cst():
    return datetime.now(pytz.timezone('America/Chicago'))


def filter_only_unshown_movies(report_df, get_datetime_cst=_get_current_datetime_cst):
    # tz-aware datetime
    cst_now = get_datetime_cst()
    # erase the tzinfo since we are given the date of the movie without a timezone (it's in CST because of Chicago)
    naive_now = date(year=cst_now.year, month=cst_now.month, day=cst_now.day)

    filtered_df = report_df[report_df["date"].dt.date >= naive_now]

    return filtered_df