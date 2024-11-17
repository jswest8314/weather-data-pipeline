import json
import boto3
import datetime
import folium
import pandas as pd
# from io import StringIO
from config import Configs

config = Configs()  # initialize the configuration object

def lambda_handler(event, context):
    bucket = config.BUCKET
    s3 = boto3.client('s3')
    status_dict = {}
    now = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    def analyze_csu():
        station_ids = config.csu_stations
        all_station_data = {}
        for station_id in station_ids:
            station_data = {}
            try:
                # FIND THE MOST RECENT FILE FOR THE STATION:
                object_list = s3.list_objects_v2(Bucket=bucket, Prefix="weather-data/csu_ag/" + f"{station_id}")[
                    "Contents"]  # return all objects and metadata
                # object_list = [{'Key': 'weather-data/csu_ag/lov01_2024_07_09_14_45_22.json', 'LastModified': datetime.datetime(2024, 7, 9, 14, 45, 24, tzinfo=tzlocal()), 'ETag': '\"17644324e7b6e3d957bcc8cb848c0266\"', 'Size': 302, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_10_14_57_17.json', 'LastModified': datetime.datetime(2024, 7, 10, 14, 57, 21, tzinfo=tzlocal()), 'ETag': '\"2d1cd02f3ae0970eafeb8846e609bbae\"', 'Size': 303, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_11_13_15_42.json', 'LastModified': datetime.datetime(2024, 7, 11, 13, 15, 47, tzinfo=tzlocal()), 'ETag': '\"5d63200f96f3d1182cdac66bbc82da08\"', 'Size': 304, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_12_09_00_09.json', 'LastModified': datetime.datetime(2024, 7, 12, 9, 0, 14, tzinfo=tzlocal()), 'ETag': '\"e25161221daf6eff39f28df7c35fb662\"', 'Size': 303, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_13_09_00_09.json', 'LastModified': datetime.datetime(2024, 7, 13, 9, 0, 14, tzinfo=tzlocal()), 'ETag': '\"8f1dad201a4b4243ef5277afa0eede56\"', 'Size': 305, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_14_09_00_09.json', 'LastModified': datetime.datetime(2024, 7, 14, 9, 0, 15, tzinfo=tzlocal()), 'ETag': '\"73ba3334a7fa07601800e44241726763\"', 'Size': 305, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_15_09_00_10.json', 'LastModified': datetime.datetime(2024, 7, 15, 9, 0, 14, tzinfo=tzlocal()), 'ETag': '\"944a0772c77b1dba417a2772c826c574\"', 'Size': 305, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_16_09_00_10.json', 'LastModified': datetime.datetime(2024, 7, 16, 9, 0, 14, tzinfo=tzlocal()), 'ETag': '\"c216165dd74c3f24977a43a5b81cef9c\"', 'Size': 304, 'StorageClass': 'STANDARD'}, {'Key': 'weather-data/csu_ag/lov01_2024_07_17_09_00_10.json', 'LastModified': datetime.datetime(2024, 7, 17, 9, 0, 14, tzinfo=tzlocal()), 'ETag': '\"0770f86be07848ae196d6c2ca4671398\"', 'Size': 303, 'StorageClass': 'STANDARD'}]
                count = len(object_list)
                filenames = [x['Key'] for x in object_list]
                modified_datetime = [x['LastModified'] for x in object_list]  # last time the file was modified
                files = dict(zip(filenames, modified_datetime))
                # storage_size = [x['Size'] for x in object_list]  # storage size in Bytes
                most_recent_data = max(modified_datetime)
                most_recent_file = list(files.keys())[list(files.values()).index(most_recent_data)]

                # PULL THE DATA FROM THE MOST RECENT FILE FOR THE STATION:
                filename = most_recent_file  # Example is: weather-data/csu_ag/lov01_2024_07_09_14_45_22.json
                response = s3.get_object(Bucket=bucket, Key=filename)
                # response =   "{'ResponseMetadata': {'RequestId': 'W53A9NA2JQ0NQ9SD', 'HostId': '/cHBcy/f94jfXny7GyxJshc1Q4ZNR/fmVgcRtvoAIn4M8KBXFelh/tzH/aMos08JWgWElXV6tuVFoXXkuwjfJA==', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '/cHBcy/f94jfXny7GyxJshc1Q4ZNR/fmVgcRtvoAIn4M8KBXFelh/tzH/aMos08JWgWElXV6tuVFoXXkuwjfJA==', 'x-amz-request-id': 'W53A9NA2JQ0NQ9SD', 'date': 'Tue, 16 Jul 2024 14:56:54 GMT', 'last-modified': 'Tue, 16 Jul 2024 09:00:14 GMT', 'etag': '\"c216165dd74c3f24977a43a5b81cef9c\"', 'x-amz-server-side-encryption': 'AES256', 'x-amz-version-id': 'cuYG2rdUgM7fn457gNlEr88olE1ukdJJ', 'accept-ranges': 'bytes', 'content-type': 'binary/octet-stream', 'server': 'AmazonS3', 'content-length': '304'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2024, 7, 16, 9, 0, 14, tzinfo=tzutc()), 'ContentLength': 304, 'ETag': '\"c216165dd74c3f24977a43a5b81cef9c\"', 'VersionId': 'cuYG2rdUgM7fn457gNlEr88olE1ukdJJ', 'ContentType': 'binary/octet-stream', 'ServerSideEncryption': 'AES256', 'Metadata': {}, 'Body': <botocore.response.StreamingBody object at 0x7f5a8f151150>}"
                data_json = json.loads(response["Body"].read())
                precip = data_json["precip"]
                station_data["raw"] = precip
                if sum(precip) < 0:
                    station_data["cumulative"] = 0
                else:
                    station_data["cumulative"] = sum(precip)
            except:
                station_data["raw"] = []
                station_data["cumulative"] = 0

            all_station_data[station_id] = station_data

        return all_station_data

    def analyze_airport():
        station_ids = config.gov_stations
        all_station_data = {}

        for station_id in station_ids:
            station_data = {}
            # try:
            # FIND THE MOST RECENT FILE FOR THE STATION:
            object_list = s3.list_objects_v2(Bucket=bucket, Prefix="weather-data/airport/")["Contents"]  # return all objects and metadata
            count = len(object_list)
            filenames = [x['Key'] for x in object_list]
            modified_datetime = [x['LastModified'] for x in object_list]  # last time the file was modified
            files = dict(zip(filenames, modified_datetime))
            # storage_size = [x['Size'] for x in object_list]  # storage size in Bytes
            most_recent_data = max(modified_datetime)
            most_recent_file = list(files.keys())[list(files.values()).index(most_recent_data)]

            # PULL THE DATA FROM THE MOST RECENT FILE FOR THE STATION:
            filename = most_recent_file  # Example is: weather-data/csu_ag/lov01_2024_07_09_14_45_22.json
            response = s3.get_object(Bucket=bucket, Key=filename)
            data_json = json.loads(response["Body"].read())

            ##########################################################################################################
            cumulative_1hr = 0
            hourly_precip = []
            for row in data_json:
                if row["precip_inches_1hr"] != "":
                    hourly_precip.append(float(row["precip_inches_1hr"]))
                    cumulative_1hr += float(row["precip_inches_1hr"])
                else:
                    hourly_precip.append(0)
            #######################################################################################################

            # station_data["raw"] = data_json
            station_data["raw"] = {"hourly_precip": hourly_precip}
            station_data["cumulative"] = cumulative_1hr

            all_station_data[station_id] = station_data



        return all_station_data

    def analyze_cocorahs():
        station_ids = config.cocorahs_stations  # Nearest four stations to my home as seen on: https://maps.cocorahs.org/
        all_station_data = {}


        for station_id in station_ids:
            station_data = {}
            # try:
            # FIND THE MOST RECENT FILE FOR THE STATION:
            object_list = s3.list_objects_v2(Bucket=bucket, Prefix="weather-data/cocorahs/" + f"{station_id}")["Contents"]  # return all objects and metadata
            count = len(object_list)
            filenames = [x['Key'] for x in object_list]
            modified_datetime = [x['LastModified'] for x in object_list]  # last time the file was modified
            files = dict(zip(filenames, modified_datetime))
            # storage_size = [x['Size'] for x in object_list]  # storage size in Bytes
            most_recent_data = max(modified_datetime)
            most_recent_file = list(files.keys())[list(files.values()).index(most_recent_data)]

            # PULL THE DATA FROM THE MOST RECENT FILE FOR THE STATION:
            filename = most_recent_file  # Example is: weather-data/csu_ag/lov01_2024_07_09_14_45_22.json
            response = s3.get_object(Bucket=bucket, Key=filename)
            data_json = json.loads(response["Body"].read())

            ##########################################################################################################
            dates = []
            precip = []
            cumulative = 0
            count = 0
            for row in data_json:
                if count>6:
                    break
                else:
                    dates.append(str(row["date"]))
                    precip.append(str(row["gauge_catch_inches"]))
                    value = row["gauge_catch_inches"].replace(".", "", 1)
                    if value.isdigit():
                        cumulative += float(row["gauge_catch_inches"])
                    elif row["gauge_catch_inches"] == "T":
                        cumulative += 0.01  # add 0.01 to the cumulative for trace events denoted by T
                    elif row["gauge_catch_inches"] == "M":
                        cumulative += 0  # add 0 to the cumulative for missing events denoted by M
                count += 1
            #######################################################################################################

            # station_data["raw"] = data_json
            station_data["raw"] = {"dates": dates, "precip": precip}
            station_data["cumulative"] = cumulative
            all_station_data[station_id] = station_data

        return all_station_data

    station_data = analyze_csu()
    airport_data = analyze_airport()
    station_data.update(airport_data)
    cocorahs_data = analyze_cocorahs()
    station_data.update(cocorahs_data)
    ###################################################################################################################
    # FOLIUM MAP!!!!
    ###################################################################################################################
    m = folium.Map(location=(40.5, -105))

    folium.Marker(
        location=[40.5762, -105.0860],
        tooltip="Station ID: FCL01",
        popup=f"{station_data['fcl01']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/fcl01_main.html

    folium.Marker(
        location=[40.56, -105.1],
        tooltip="Station ID: FTC04",
        popup=f"{station_data['ftc04']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/ftc04

    folium.Marker(
        location=[40.61, -105],
        tooltip="Station ID: FTC02",
        popup=f"{station_data['ftc02']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/ftc02

    folium.Marker(
        location=[40.37, -105],
        tooltip="Station ID: JCN01",
        popup=f"{station_data['jcn01']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/jcn01

    folium.Marker(
        location=[40.42, -105.1],
        tooltip="Station ID: LOV01",
        popup=f"{station_data['lov01']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/lov01

    folium.Marker(
        location=[40.45, -105.01],
        tooltip="Station ID: KFNL",
        popup=f"{station_data['KFNL']}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)  # Details here: https://coagmet.colostate.edu/station/lov01

    folium.Marker(
        location=[40.537, -105.02966],
        tooltip="CO-LR-290",
        popup=f"{station_data["CO-LR-290"]}",
        icon=folium.Icon(color="blue")
    ).add_to(m)

    folium.Marker(
        location=[40.510567, -105.059309],
        tooltip="CO-LR-1304",
        popup=f"{station_data["CO-LR-1304"]}",
        icon=folium.Icon(color="blue")
    ).add_to(m)

    folium.Marker(
        location=[40.5049648938451, -105.027796362618],
        tooltip="CO-LR-1331",
        popup=f"{station_data["CO-LR-1331"]}",
        icon=folium.Icon(color="blue")
    ).add_to(m)

    folium.Marker(
        location=[40.53172, -105.02665],
        tooltip="CO-LR-1336",
        popup=f"{station_data["CO-LR-1336"]}",
        icon=folium.Icon(color="blue")
    ).add_to(m)


    # create a geojson for a cholorpleth map here: https://geojson.io/#map=11.3/40.4805/-104.9971
    # Make a chloropleth map for each region: https://python-visualization.github.io/folium/latest/reference.html#folium.features.Choropleth
    stations = list(station_data.keys())
    precip = [station_data[x]['cumulative'] for x in stations]
    d = {'stations': stations, 'precip': precip}
    df = pd.DataFrame(d)
    print(df)
    folium.features.Choropleth(geo_data="station_areas.geojson",
                               data=df,
                               columns=["stations", "precip"],
                               key_on="feature.properties.stationid",
                               fill_color="PuBu",
                               highlight=True,
                               ).add_to(m)

    # ADD TOOLTIPS TO CHLOROPLETH MAP: https://stackoverflow.com/questions/70471888/text-as-tooltip-popup-or-labels-in-folium-choropleth-geojson-polygons

    # SAVE HTML TO MEMORY, REFERENCE: https://stackoverflow.com/questions/52086739/saving-html-in-memory-to-s3-aws-python-boto3
    m.save('/tmp/map.html')  # Note that only /tmp is writeable on Lambda instance
    with open('/tmp/map.html', 'rb') as f:
        s3.put_object(Bucket=bucket, Key=f'maps/map_{now}.html', Body=f)  # Upload as bytes

    return {
        'statusCode': 200,
        'all_station_data': station_data,
        'bucketSource': f"{bucket}"
    }







# 
# def analyze_csu():
# 
#     df_ftc = pd.DataFrame(data={'Date':data['time'],'ftc04': data['precip']})
#     # # print(df)
#     # r = requests.get('https://coagmet.colostate.edu/data/nw/daily/lov01.json?fields=precip')
#     # data = r.json()
#     # df_lov = pd.DataFrame(data={'Date':data['time'],'lov01': data['precip']})
#     # # print(df)
#     # df_full = pd.merge(df_ftc,df_lov,left_on='Date',right_on='Date')
#     # df_full.replace(to_replace=-999, value=np.nan, inplace=True) #-999 in this dataset indicates no reading
#     # print(df_full)
# 
# def analyze_cocorahs():
#     station_ids = ["CO-LR-290", "CO-LR-1304", "CO-LR-1331",
#                    "CO-LR-1336"]  # Nearest four stations to my home as seen on: https://maps.cocorahs.org/
#     def precip(station_id):
#         # PULL DATA FROM DATABASE FOR THE LAST 4 DAYS AND LAST 8 DAYS:
#         today = dt.date.today()
#         today_minus3 = today - dt.timedelta(days=4)
#         today_minus8 = today - dt.timedelta(days=8)
#         con = sqlite3.connect("rain_data.db")
#         cur = con.cursor()
#         # TODO: NEED TO ACCOUNT FOR DAYS WITH NO DATA
#         precip_past_3days = cur.execute("SELECT gauge_catch_inches FROM cocorahs WHERE date > ? AND station_id = ?",
#                                         (today_minus3, station_id)).fetchall()
#         precip_past_week = cur.execute("SELECT gauge_catch_inches FROM cocorahs WHERE date > ? AND station_id = ?",
#                                        (today_minus8, station_id)).fetchall()
# 
#         # SUM PRECIPITATION FOR THE PAST 3 DAYS
#         cumulative_precip_3days = 0
#         for i in range(len(precip_past_3days)):
#             if precip_past_3days[i][0].lower() == "t":
#                 cumulative_precip_3days += 0.01
#             else:
#                 try:
#                     cumulative_precip_3days += int(precip_past_3days[i][0])
#                 except:
#                     cumulative_precip_3days += 0
# 
#         # SUM PRECIPITATION FOR THE PAST 7 DAYS
#         cumulative_precip_week = 0
#         for i in range(len(precip_past_week)):
#             if precip_past_week[i][0].lower() == "t":
#                 cumulative_precip_week += 0.01
#             else:
#                 try:
#                     cumulative_precip_week += int(precip_past_week[i][0])
#                 except:
#                     cumulative_precip_week += 0
# 
#         return cumulative_precip_3days, cumulative_precip_week
# 
# def analyze_airport():
#     def precip():
#         # PULL DATA FROM DATABASE FOR THE LAST 4 DAYS AND LAST 8 DAYS:
#         today = dt.date.today()
#         today_minus3 = today - dt.timedelta(days=4)
#         today_minus8 = today - dt.timedelta(days=8)
#         cur = sqlite3.connect("rain_data.db").cursor()
#         precip_past_3days = cur.execute("SELECT precip_inches_1hr FROM airport WHERE date > ?",
#                                         (today_minus3,)).fetchall()
#         precip_past_week = cur.execute("SELECT precip_inches_1hr FROM airport WHERE date > ?",
#                                        (today_minus8,)).fetchall()
# 
#         # SUM PRECIPITATION FOR THE PAST 3 DAYS
#         cumulative_precip_3days = 0
#         for i in range(len(precip_past_3days)):
#             if precip_past_3days[i][0] != '':
#                 cumulative_precip_3days += int(precip_past_3days[i][0])
# 
#         # SUM PRECIPITATION FOR THE PAST 7 DAYS
#         cumulative_precip_week = 0
#         for i in range(len(precip_past_week)):
#             if precip_past_week[i][0] != '':
#                 cumulative_precip_week += int(precip_past_week[i][0])
# 
#         return cumulative_precip_3days, cumulative_precip_week
# 
# 
#     # return {
#     #     'statusCode': 200,
#     #     'statusByStation': f"{status_dict}",
#     #     'bucketDestination': f"{bucket}"
#     # }
