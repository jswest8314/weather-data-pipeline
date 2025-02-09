from generate_station_list import generate_station_list
import json

def test_generate_station_list():
    with open("test_data/example_stations.json") as f:
        station_response = json.load(f)
    stations_list = generate_station_list("test_data/example_polygon.geojson", station_response)

    # We should only find one station in the specified area
    assert stations_list[0]["properties"]["stationIdentifier"] == "3CO4"
    assert stations_list[0]["geometry"]["coordinates"] == [-105.01861, 37.07085]
