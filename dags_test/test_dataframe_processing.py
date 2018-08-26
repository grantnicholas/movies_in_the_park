from spothero_challenge.dataframe_processing import _extract_lat_and_long


def test_extract_lat_and_long():
    lat = 86.0000
    long = -50.0000
    test_location_dict_from_pandas_frame = {"coordinates": [long, lat]}
    (out_long, out_lat) = _extract_lat_and_long(test_location_dict_from_pandas_frame)

    assert lat == out_lat
    assert long == out_long

