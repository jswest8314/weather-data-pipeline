# Noting National Weather Service (NWS) Resources

## Finding Weather Stations
* [Weather Station Map](https://www.wrh.noaa.gov/map/?obs=true&wfo=lox)
* [NWS API Find Stations](https://www.weather.gov/documentation/services-web-api#/default/obs_stations)
  * Can filter by state (i.e., https://api.weather.gov/stations?state=CO)


## Pulling Weather Station Data
* [NWS API Station Observations](https://www.weather.gov/documentation/services-web-api#/default/station_observation_list)
  * Example: https://api.weather.gov/stations/{station-id}/observations
  * Can add start and end times to pull historical data!
    * https://api.weather.gov/stations/{station-id}/observations?start=2020-05-14T05:40:08-00:00
* At first pass, it seems that some stations on the weather station map (above) cannot be accessed with the API but the 
last 7 days of data can be accessed on the website.
  * e.g.: https://www.weather.gov/wrh/timeseries?site=G4589&hours=168

