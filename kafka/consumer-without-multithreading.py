from kafka import KafkaConsumer
import json
from datetime import datetime, timezone
import hashlib
import os
import sys
import threading
import queue
from dotenv import load_dotenv
from pathlib import Path
import os

sys.path.append(".")
from snowflake.connection import insert_arrival_, insert_disruption_data

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

import boto3
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY")
)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

BUCKET_NAME = "tfl"

def get_hash(data) -> str:
    json_str = json.dumps(data, sort_keys=True,cls=DateTimeEncoder)
    return hashlib.sha256(json_str.encode()).hexdigest()

def save_new_json(data, type_,category, key=None):
    S3_KEY = f"data/{type_}/{category}/"
    hash_value = get_hash(data)
    hash_file =  f"hash_{hash_value}.json"
    if type_ == "raw":
        try:
            print("checkin raw data")
            if s3.head_object(Key=S3_KEY+hash_file, Bucket=BUCKET_NAME):
                print(f"[SKIP] Duplicate snapshot for {category} (hash: {hash_value[:8]})")
                return None
        except:
            pass
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:21]
        file_path = f"snapshot_{timestamp}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        with open(hash_file, "w") as f:
            pass
        s3.upload_file(
            Filename=hash_file,Key=(S3_KEY+hash_file),Bucket=BUCKET_NAME
        )
        #hash_file.touch()
        S3_KEY = S3_KEY + file_path
        s3.upload_file(
            Filename=file_path,Key=S3_KEY,Bucket=BUCKET_NAME
        )
        
        print(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{S3_KEY} at timestamp {timestamp}")
        os.remove(file_path)
        os.remove(hash_file)
        return S3_KEY
    if type_ == "cleaned":
        file_name = f"{category}_cleaned.json"
        KEY = str(key).replace("raw", "cleaned")#f"data/raw/{category}/" + file_name
        with open(file_name, "w") as f:
            json.dump(data,f,indent=2,cls=DateTimeEncoder)
        s3.upload_file(
            Filename=file_name,Key=KEY,Bucket=BUCKET_NAME
        )
        print(f"Uploaded {file_name} to s3://{BUCKET_NAME}/{KEY} at timestamp {datetime.now()}")
        os.remove(file_name)
    

def transform_arrival(data_):
    cleaned_data = []
    for data in data_:
        id = data.get("id")
        vehicle_id = data.get("vehicleId")
        line_id = data.get("lineId")
        station_name = data.get("stationName")
        naptan_id = data.get("naptanId")
        direction = data.get("direction")
        bearing   = data.get("bearing", None)
        expected_arrival = data.get("expectedArrival") 
        destination_name = data.get("destinationName")
        time_to_station = data.get("timeToStation")
        platform_name = data.get("platformName")
        current_location = data.get("currentLocation")
        mode_name = data.get("modeName")
        expected_arrival = datetime.strptime(expected_arrival,"%Y-%m-%dT%H:%M:%SZ") if expected_arrival else None
        cleaned_data.append({
            "id": id, "vehicle_id": vehicle_id, "line_id": line_id, "station_name": station_name,
            "naptan_id": naptan_id, "direction": direction, "expected_arrival": expected_arrival,
            "destination_name": destination_name, "time_to_station": time_to_station, "bearing": bearing,
            "platform_name": platform_name, "current_location": current_location, "mode_name": mode_name
        })
    insert_arrival_(cleaned_data)
    return cleaned_data

def transform_disruption(data):
    disruptions = []
    for d in data:
        disruptions.append({
            "id": d.get("id"),
            "line_id": d.get("lineId"),
            "category": d.get("category"),
            "type": d.get("type"),
            "description": d.get("description"),
            "is_planned": d.get("isPlanned"),
            "affected_routes": [route.get("name") for route in d.get("affectedRoutes", [])],
            "affected_stops": [stop.get("naptanId") for stop in d.get("affectedStops", [])],
            "start_time": d.get("startDateTime"),
            "end_time": d.get("endDateTime"),
            "last_update": d.get("lastUpdate")
        })
    return disruptions

topics = ["arrival", "disruption"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers = '127.0.0.1:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Create a thread-safe queue
save_queue = queue.Queue()

def worker():
    """Worker thread to process items from the queue."""
    while True:
        try:
            # Get an item from the queue
            task = save_queue.get()
            if task is None:  # Exit signal
                break
            
            # Unpack the task
            data, type_, category, key = task
            
            # Call the save_new_json function
            key = save_new_json(data, type_, category, key)
            
            # If the task is for raw data, enqueue the cleaned data save task
            if type_ == "raw":
                if category == "arrivals":
                    cleaned_data = transform_arrival(data)
                elif category == "disruptions":
                    cleaned_data = transform_disruption(data)
                else:
                    cleaned_data = None
                
                if cleaned_data:
                    save_queue.put((cleaned_data, "cleaned", category, key))
        finally:
            save_queue.task_done()

# Start worker threads
num_threads = 4  # Adjust the number of threads as needed
threads = []
for _ in range(num_threads):
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    threads.append(t)

# Add data to the queue
def enqueue_save(data, type_, category, key=None):
    """Add a save task to the queue."""
    save_queue.put((data, type_, category, key))

# Example usage in the Kafka consumer loop
for message in consumer:
    if message.topic == "arrival":
        print("from arrival: ", json.dumps(message.value)[:40] + "... (truncated)")
        enqueue_save(message.value, "raw", category="arrivals")
    if message.topic == "disruption":
        print("from disruption: ", json.dumps(message.value)[:40] + "... (truncated)")
        enqueue_save(message.value, "raw", category="disruptions")

# Wait for all tasks to complete
save_queue.join()

# Stop worker threads
for _ in threads:
    save_queue.put(None)  # Send exit signal
for t in threads:
    t.join()