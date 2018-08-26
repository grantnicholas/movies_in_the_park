from spothero_challenge.dataframe_processing import (
    _extract_lat_and_long, enrich_movies, enrich_titles_and_ratings, filter_only_unshown_movies,
    _remove_dupe_movies_by_title, _min_max_normalize
)
import pandas as pd
from datetime import datetime
import pytz


def get_row_count(df):
    return df.shape[0]


def test_extract_lat_and_long():
    lat = 86.0000
    long = -50.0000
    test_location_dict_from_pandas_frame = {"coordinates": [long, lat]}
    (out_long, out_lat) = _extract_lat_and_long(test_location_dict_from_pandas_frame)

    assert lat == out_lat
    assert long == out_long


def test_parse_movies_json_does_not_fail_and_does_not_drop_records(get_resource_file):
    path = get_resource_file("movies.json")
    movies_df = pd.read_json(path)

    out_df = enrich_movies(movies_df)
    # row count matches
    assert get_row_count(movies_df) == get_row_count(out_df)


def test_enrich_titles_and_ratings(get_resource_file):
    basics_path = get_resource_file("title.basics.tsv.sampled")
    ratings_path = get_resource_file("title.ratings.tsv.sampled")

    titles_df = pd.read_csv(basics_path, delimiter='\t')
    ratings_df = pd.read_csv(ratings_path, delimiter='\t')

    out_df = enrich_titles_and_ratings(titles_df, ratings_df)

    assert get_row_count(out_df) > 0
    assert get_row_count(out_df[out_df["titleType"] == "movie"]) == get_row_count(out_df)


def test_filter_only_unshown_movies():
    cst = pytz.timezone('America/Chicago')

    test_df = pd.DataFrame.from_dict({
        "date": [
            pd.datetime(year=2017, month=8, day=10),
            pd.datetime(year=2018, month=7, day=10),
            pd.datetime(year=2018, month=7, day=30),
            pd.datetime(year=2018, month=8, day=1),
            pd.datetime(year=2018, month=8, day=2)
        ]
    })
    mock_get_current_datetime = lambda: datetime(year=2018, month=8, day=1, tzinfo=cst)

    out_df = filter_only_unshown_movies(test_df, mock_get_current_datetime)

    # Only two records after the given date
    assert get_row_count(out_df) == 2


def test_remove_dupes_by_title():
    df = pd.DataFrame.from_dict({
        "primaryTitle": ["Jaws", "Frozen", "Jaws", "Nightmare on Elm Street"],
        "numVotes": [100, 5000, 3000, 5]
    })
    out_df = _remove_dupe_movies_by_title(df, "primaryTitle")

    out_data = list(out_df.sort_values(["numVotes"], ascending=True).itertuples(index=False))
    assert out_data == [
        ("Nightmare on Elm Street", 5),
        ("Jaws", 3000),
        ("Frozen", 5000)
    ]


def test_min_max_normalize_df():
    df = pd.DataFrame.from_dict({
        "dev_temp": [0, 5, 10],
        "dev_precip": [0.10, 0.05, 0.0]
    })

    _min_max_normalize(df, columns_to_normalize=["dev_temp", "dev_precip"])

    out_data = list(df.itertuples(index=False))

    assert out_data == [
        (0, 0.10, 0, 1),
        (5, .05, .5, .5),
        (10, 0.0, 1, 0)
    ]