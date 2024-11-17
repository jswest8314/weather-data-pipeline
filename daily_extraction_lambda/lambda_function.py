import json
import requests  #TODO: evaluate replacing requests library with urllib since urllib is included on AWS lambda
from bs4 import BeautifulSoup
import datetime as dt
import boto3
from config import Configs

config = Configs()  # initialize the configuration object


def lambda_handler(event, context):
    now = dt.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    bucket = config.BUCKET
    status_dict = {}
    s3 = boto3.client('s3')

    def gather_csu():
        station_ids = config.csu_stations
        for station_id in station_ids:
            extraction_success = 0
            try:
                # PULL DATA FROM "https://coagmet.colostate.edu/"
                url = f'https://coagmet.colostate.edu/data/nw/daily/{station_id}.json?fields=precip'
                r = requests.get(url)
                data = r.json()
                extraction_success = 1
            except:
                pass

            upload_success = 0
            try:
                data_filename = "weather-data/csu_ag/" + station_id + "_" + now + ".json"
                uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
                s3.put_object(Bucket=bucket, Key=data_filename, Body=uploadByteStream)
                upload_success = 1
            except:
                pass

            status_dict[station_id] = {"extraction_success": extraction_success, "upload_success": upload_success}

    def gather_cocorahs():
        column_labels = ["station_id", "map_link", "date", "obs_time", "gauge_catch_inches", "snowfall_depth_inches",
                         "snow_water_equivalent_inches", "snowpack_depth_inches", "snowpack_water_equivalent_inches",
                         "Flooding", "Notes"]
        station_ids = config.cocorahs_stations
        for station_id in station_ids:
            extraction_success = 0
            try:
                # PULL DATA FROM "https://dex.cocorahs.org/stations/station_id/obs-tables"
                url = 'https://dex.cocorahs.org/stations/' + station_id + '/obs-tables'
                r = requests.get(url)
                soup = BeautifulSoup(r.content, "html.parser")
                all_tables = soup.find_all('table')
                data_table = all_tables[1]
                rows = data_table.find_all('tr')
                extraction_success = 1
            except:
                today = dt.date.today().strftime("%Y-%m-%d")
                failure_string = f"<table><tr><td><a><i></i></a></td><td>{today}</td><td>failed data pull</td><td><span></span></td><td><span></span></td><td><span></span></td><td><span></span></td><td><span></span></td><td></td><td></td></tr></table>"
                rows = BeautifulSoup(failure_string, "lxml").find('table').find_all('tr')

            # PARSE DATA INTO DICTIONARY FORM FOR SERIALIZATION
            data_parsing_success = 0
            try:
                data = []
                for row in rows:
                    cols = row.find_all('td')
                    values = [ele.text.strip() for ele in cols]
                    row_data = [ele for ele in values]  # use "[ele for ele in values if ele]" to get rid of empty columns
                    row_data.insert(0, station_id)
                    dict_i = dict(zip(column_labels, row_data))
                    data.append(dict_i)
                    data_parsing_success = 1
            except:
                pass

            upload_success = 0
            try:
                data_filename = "weather-data/cocorahs/" + station_id + "_" + now + ".json"
                uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
                s3.put_object(Bucket=bucket, Key=data_filename, Body=uploadByteStream)
                upload_success = 1
            except:
                pass

            status_dict[station_id] = {"extraction_success": extraction_success, "data_parsing_success": data_parsing_success, "upload_success": upload_success}

    def gather_airport():
        station_ids = config.gov_stations
        for station_id in station_ids:
            extraction_success = 0
            try:
                # PULL DATA FROM https://forecast.weather.gov/data/obhistory/KFNL.html
                r = requests.get('https://forecast.weather.gov/data/obhistory/KFNL.html')
                soup = BeautifulSoup(r.content, "html.parser")
                all_tables = soup.find_all('table')
                data_table = all_tables[0]
                rows = data_table.find_all('tr')
                extraction_success = 1
            except:
                # CREATE A BLANK ROW WITH AN ERROR MESSAGE FOR THE DATABASE IF THE DATA PULL WAS UNSUCCESSFUL
                failure_string = "<table><tr><td>0</td><td>failed data pull</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr></table>"
                rows = BeautifulSoup(failure_string, "lxml").find('table').find_all('tr')

            data_parsing_success = 0
            # PARSE DATA INTO DICTIONARY FORM FOR SERIALIZATION
            ##### PREP DATES FOR PARSING DATA
            today = dt.date.today()
            today_minus1 = today - dt.timedelta(days=1)
            today_minus2 = today - dt.timedelta(days=2)
            today_minus3 = today - dt.timedelta(days=3)
            column_labels = ["station_id", "date", "day_raw", "time_mst", "wind_mph", "visibility_miles", "weather",
                             "sky_condition", "temperature_air", "temperature_dewpoint", "temperature_6hr_max",
                             "temperature_6hr_min", "relative_humidity", "temperature_windchill", "temperature_heat_index",
                             "pressure_altimeter_inches", "pressure_sea_level_mbar", "precip_inches_1hr",
                             "precip_inches_3hr", "precip_inches_6hr"]
            try:
                data = []
                for row in rows:
                    cols = row.find_all('td')
                    values = [ele.text.strip() for ele in cols if len(cols) > 1]
                    if len(values) == 0:
                        pass  # Skip empty rows
                    else:
                        # INSERT DATE INTO ROW NEXT TO CORRESPONDING DAY
                        day = int(values[0])
                        if day == today_minus1.day:
                            values.insert(0, today_minus1.strftime("%Y-%m-%d"))
                        elif day == today_minus2.day:
                            values.insert(0, today_minus2.strftime("%Y-%m-%d"))
                        elif day == today_minus3.day:
                            values.insert(0, today_minus3.strftime("%Y-%m-%d"))
                        else:
                            values.insert(0, today.strftime("%Y-%m-%d"))  # PUT THIS LAST TO CATCH INGESTION DATE AS A FAIL SAFE

                        values.insert(0, "KFNL_loveland")
                        dict_i = dict(zip(column_labels, values))  # zip labels and values into a dictionary
                        data.append(dict_i)  # append dictionary for this row to the list to import into the sqlite database
                data_parsing_success = 1
            except:
                pass

            upload_success = 0
            try:
                data_filename = "weather-data/airport/" + station_id + "_" + now + ".json"
                uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
                s3.put_object(Bucket=bucket, Key=data_filename, Body=uploadByteStream)
                upload_success = 1
            except:
                pass

            status_dict[station_id] = {"extraction_success": extraction_success,
                                       "data_parsing_success": data_parsing_success, "upload_success": upload_success}

    gather_csu()
    gather_cocorahs()
    gather_airport()

    return {
        'statusCode': 200,
        'statusByStation': f"{status_dict}",
        'bucketDestination': f"{bucket}"
    }
