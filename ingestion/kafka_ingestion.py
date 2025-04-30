import requests
import json
import datetime
from dotenv import load_dotenv
from pathlib import Path
import os


env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
BASE_URL = os.environ["BASE_URL"]
app_key = os.environ["APP_KEY"]

def get_arrivals(naptan_id):
    url = BASE_URL + f"/StopPoint/{naptan_id}/Arrivals?api_key=" + app_key
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
        return data

def get_disruptions_for_line(line_id):
    url = f"{BASE_URL}/Line/{line_id}/Disruption?app_key={app_key}"
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return data
    except Exception as e:
        print(f"Error fetching disruptions for line {line_id}: {e}")
        return None

