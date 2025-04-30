import requests
import json, os
from urllib3.exceptions import HeaderParsingError
import warnings
warnings.filterwarnings("ignore", message=".*HeaderParsingError.*")


BASE_URL = os.environ["BASE_URL"]
app_key = os.environ["APP_KEY"]

# line_json = None
# path = "line_stop.json"
# with open(path, "r") as f:
#     line_json = json.load(f)
# line_ids = []
# naptan_idx = []
# for key, values in line_json.items():
#     for key, values in line_json[key].items():
#         line_ids.append(key)
#         naptan_idx.append(values)

# def get_stops_for_line(line_id, naptan_id=None):
#     url = f"{BASE_URL}/Line/{line_id}/StopPoints?app_key={app_key}"
#     #url = f"{BASE_URL}/StopPoint/{naptan_id}?app_key={app_key}"
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         stop_points = {}
#         for stop in data:
#             if stop['naptanId'] in naptan_idx:
#                 stop_points[stop['naptanId']] = stop
#         return stop_points
#     except (requests.exceptions.RequestException, HeaderParsingError) as e:
#         print(f"Error fetching stops for line {line_id}: {e}")
#         return {}

def get_timetable(line_id, stop_point):
    url =  f"{BASE_URL}/Line/{line_id}/Timetable/{stop_point}?app_key={app_key}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        if "disambiguation" in data.keys():
            data = []
            for option in data["disambiguationOptions"]:
                uri = BASE_URL+ "/" + option["uri"]
                r = requests.get(uri)
                r.raise_for_status()
                data.append(r.json())
            return data
        return [data]
    except (Exception, HeaderParsingError) as e:
        #add loggings here
        print("Error fetching data ",e )
        return []

def get_stop_point_metadata(naptan_id):
    url = f"{BASE_URL}/StopPoint/{naptan_id}?app_key={app_key}"
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return data
    except (Exception, HeaderParsingError) as e:
        print(f"Error fetching metadata for stop {naptan_id}: {e}")
        return None
