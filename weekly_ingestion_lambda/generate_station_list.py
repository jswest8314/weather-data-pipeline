import json
import os
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

def generate_station_list(geojson_file: str, station_response: str):
    """
        This function takes a Polygon defined in .geojson file and returns a list of NWS station IDs that are inside
        that polygon. Optional arguments include US States to help filter the location in which Steps are:
        1. Open the .geojson file and extract the Polygon definition
        2. Call the NWS API to find Stations https://www.weather.gov/documentation/services-web-api#/default/obs_stations
            3.1 Iterate through the API response and move to further pages.
            3.2 Iterate through each weather station and determine if is within the Polygon
    """

    # Step 1: Open the .geojson file and create a list of shapely.geometry.polygon object(s)
    area = []
    with open(geojson_file, "r") as f:
        d = json.load(f)
        for feature in d["features"]:
            all_coordinates = feature["geometry"]["coordinates"]
            # print(all_coordinates)
            for coordinate_group in all_coordinates:
                # Create a polygon object
                polygon = Polygon(coordinate_group)
                # Add the polygon object to the area list
                area.append(polygon)
    # print(area)

    # Step 2: Iterate through the stations in the station_response and determine if the station is in a polygon
    stations_list = []
    for feature in station_response["features"]:
        # print(feature)
        # Create a shapely.geometry Point object for the station
        station_coordinates = feature["geometry"]["coordinates"]
        # print(station_coordinates)
        point = Point(station_coordinates[0], station_coordinates[1])  # create point, note input is long, lat

        # Determine if the point is in any polygon
        for polygon in area:
            # print(polygon)
            if polygon.contains(point) or point.within(polygon):  # check if polygon contains point
                # solver_method = {"polygon": polygon.contains(point), "point": point.within(polygon)}
                # print(solver_method)
                # print(f"Station {feature["properties"]["stationIdentifier"]} exists in the polygon!") # Should find 3CO4
                stations_list.append(feature)

    return stations_list


with open("test_data/example_stations.json") as f:
    station_response = json.load(f)

stations_list = generate_station_list("test_data/example_polygon.geojson", station_response)
print(stations_list)

# import geojson
# with open("test_data/example_polygon.geojson") as f:
#     polygon_ex = str(json.load(f))
#
# geojson_object = geojson.loads('{"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"coordinates": [[[-105.03658592255513, 37.08036899710187], [-105.03658592255513, 37.047888554137], [-104.99228676390703, 37.047888554137], [-104.99228676390703, 37.08036899710187], [-105.03658592255513, 37.08036899710187]]], "type": "Polygon"}}]}')
# print(type(geojson_object))
# print(geojson_object)
# print(geojson_object[0])
# print(geojson_object[0]["geometry"])
# print(geojson_object[0]["geometry"]["coordinates"])

