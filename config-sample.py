
class Configs():
    def __init__(self):
        self.BUCKET = "example_bucket"  # the name of the S3 bucket to which the data will be written
        self.csu_stations = ["station02"]  # https://coagmet.colostate.edu/station/selector
        self.cocorahs_stations = ["state-county-stationId"]  # https://maps.cocorahs.org/
        self.gov_stations = ["stationId"]  # https://www.weather.gov/
