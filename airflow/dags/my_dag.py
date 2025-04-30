from datetime import datetime, timedelta
from datetime import timezone
import json, os, pathlib
import hashlib
from airflow import DAG
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow_ingestion import get_stop_point_metadata, get_timetable#, get_stops_for_line

from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import warnings
from urllib3.exceptions import InsecureRequestWarning, HeaderParsingError

warnings.simplefilter("ignore", HeaderParsingError)


BUCKET_NAME = os.environ["BUCKET_NAME"]
line_json = None
path = os.path.join(os.environ["AIRFLOW_HOME"],"plugins","line_stop.json")
with open(path, "r") as f:
    line_json = json.load(f)
line_ids = []
naptan_ids = []
for key, values in line_json.items():
    for key, values in line_json[key].items():
        line_ids.append(key)
        naptan_ids.append(values)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def get_hash(data) -> str:
    json_str = json.dumps(data, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()

def save_new_json(timestamp, data, category: str):
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    print(s3_hook.get_conn())
    #folder = Path(f"data/raw/{category}")
    #folder.mkdir(parents=True, exist_ok=True)
    S3_KEY = f"data/raw/{category}/"
    hash_value = get_hash(data)
    hash_file =  f"hash_{hash_value}.json"
    try:
        if s3_hook.check_for_key(S3_KEY+hash_file, bucket_name=BUCKET_NAME):
            logging(f"[SKIP] Duplicate snapshot for {category} (hash: {hash_value[:8]})")
            return None
    except:
        pass
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:21]
    file_path = f"snapshot_{timestamp}.json"
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    with open(hash_file, "w") as f:
        pass
    s3_hook.load_file(
        filename=hash_file,key=(S3_KEY+hash_file),bucket_name=BUCKET_NAME,replace=True
    )
    #hash_file.touch()
    S3_KEY = S3_KEY + file_path
    s3_hook.load_file(
        filename=file_path,key=S3_KEY,bucket_name=BUCKET_NAME,replace=True
    )
    logging.info(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{S3_KEY} at timestamp {timestamp}")
    os.remove(file_path)
    return S3_KEY

default_args = {
    "owner": "abdulsalam",
    "retries": 5,
    "retry_delay": timedelta(minutes=1)
}


def array_construct(array):
    new_array = []
    b = "ARRAY_CONSTRUCT("
    for arr in array:
        new_array.append("'" + arr + "'")
    d = ",".join(new_array)
    return b + d + ")"

def get_api_data(**kwargs):
    ed = kwargs.get("execution_date")
    ed = ed.strftime("%Y%m%d_%H%M%S")
    ti = kwargs.get("ti")
    sp_keys = []
    tt_keys = []
    for line_id, naptan_idx in zip(line_ids,naptan_ids):
        for naptan_id in naptan_idx:
            _ = get_stop_point_metadata(naptan_id)
            stop_point_key = save_new_json(ed, data=_, category="stop_points")
            sp_keys.append(stop_point_key) if stop_point_key else None
            _ = get_timetable(line_id,naptan_id)
            for res in _:
                timetable_key = save_new_json(ed, data=res, category="timetables")
                tt_keys.append(timetable_key) if timetable_key else None
    
    ti.xcom_push(key="sp-keys", value=sp_keys)
    ti.xcom_push(key="tt-keys", value=tt_keys)

def save_cleaned_data(hook,data, key, category):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:21]
    file_name = f"{category}_cleaned_{timestamp}.json"
    KEY = str(key).replace("raw", "cleaned")#f"data/raw/{category}/" + file_name
    with open(file_name, "w") as f:
        json.dump(data,f,indent=2,cls=DateTimeEncoder)
    hook.load_file(
        filename=file_name,key=KEY,bucket_name=BUCKET_NAME,replace=True
    )
    logging.info(f"Uploaded {file_name} to s3://{BUCKET_NAME}/{KEY} at timestamp {timestamp}")
    os.remove(file_name)
    return KEY

def transform_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id="minio_conn") 
    sp_keys = kwargs["ti"].xcom_pull(task_ids="api_call_static_data", key="sp-keys")
    tt_keys = kwargs["ti"].xcom_pull(task_ids="api_call_static_data", key="tt-keys")
    ti = kwargs["ti"]
    new_sp_keys = []
    new_tt_keys = []

    for key in sp_keys:
        file = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
        data = json.loads(file)
        
        cleaned_data = {
            "naptan_id": data.get("naptanId"),
            "stop_point_id": data.get("id"),
            "common_name": data.get("commonName"),
            "lat": data.get("lat"),
            "lon": data.get("lon"),
            "indicator": data.get("indicator"),
            "zone": data.get("zone"),
            "borough": data.get("borough"),
            "modes": data.get("modes", []),
            "stop_type": data.get("stopType")
        }
        new_key = save_cleaned_data(s3_hook, cleaned_data, key,category="stop_points") 
        new_sp_keys.append(new_key) 
        
    
    for key in tt_keys:
        file = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
        data = json.loads(file)
        print("raw data: ", data.keys())
        if "disambiguation" in data.keys():
            print("disamb")
            continue
        direction = data["direction"]
        line_id = data["lineId"]
        schedule =  {s["name"]: s for s in data["timetable"]["routes"][0]["schedules"]}
        station_intervals = {si["id"]: si["intervals"] for si in  data["timetable"]["routes"][0]["stationIntervals"]}
        retrieved_date_str = str(key).split("/")[-1].split(".")[0].replace("snapshot_","")
        retrieved_at = datetime.strptime(retrieved_date_str, "%Y%m%d_%H%M%S_%f").strftime("%Y-%m-%d %H:%M:%S")
        scheduled_arrival_time = None #calc during analysis since it depends on the day
        scheduled_departure_time = None #calc during analysis since it depends on the day
        from_stop_point_id = data["timetable"]["departureStopId"]
        stop_point_sequence = []
        for interval in data["timetable"]["routes"][0]["stationIntervals"][-1]["intervals"]:
            stop_point_sequence.append(interval["stopId"])
        
        #station_intervals[interval_id]

        
        timetable = {}
        try:
            for k, value in schedule.items():
                day_of_week = k
                # journeys = [value["firstJourney"]]
                # journeys.extend(value["knownJourneys"][1:])
                # journeys.append(value["lastJourney"])
                journeys = value["knownJourneys"]          
                timetable[day_of_week] = {}
                timetable[day_of_week]["schedule_time"] = []
                timetable[day_of_week]["time_intervals"] = []
                departure_times = []
                time_intervals = []
                for journey in journeys:
                    h = journey["hour"]
                    if h.startswith('24'):
                        h = '00'
                    if int(h) > 24:
                        h = str(int(h) - 24)
                    m = journey["minute"]
                    interval_id = str(journey["intervalId"])
                    dt = datetime.strptime(f"{h}:{m}", "%H:%M")
                    departure_times.append(dt)
                    time_intervals.append([int(i["timeToArrival"]) for i in station_intervals[interval_id]]) #will be list of list
                timetable[day_of_week]["schedule_time"] = departure_times
                timetable[day_of_week]["time_intervals"] = time_intervals
                
                    # for interval in station_intervals[interval_id]:
                    #     id = interval["stopId"] #get the common name 
                    #     arrival = interval["timeToArrival"]
                    #     arrival_time = dt + timedelta(minutes=arrival)
                    #     if id in timetable[day_of_week].keys():
                    #         timetable[day_of_week][id].append(arrival_time)
                    #     else:
                    #         timetable[day_of_week][id] = [arrival_time]
            cleaned_data = {
            "direction": direction, "line_id": line_id,"from_stop_id":from_stop_point_id ,"retrieved_at": retrieved_at, "stop_point_sequence": stop_point_sequence,
            "schedules": timetable
            }
            new_key = save_cleaned_data(s3_hook,cleaned_data,key, category="timetable") 
            new_tt_keys.append(new_key)
        except:
            logging.error(f"Error processing file: {key}")
    
    ti.xcom_push(key="sp-keys", value=new_sp_keys)
    ti.xcom_push(key="tt-keys", value=new_tt_keys)
            
def load_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id="minio_conn")
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    print("snowflake conn: ", conn)
    conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'Europe/London';")
    cursor = conn.cursor()
    
    
    sp_keys = kwargs["ti"].xcom_pull(task_ids="transform_data", key="sp-keys") #for cleaned data
    tt_keys = kwargs["ti"].xcom_pull(task_ids="transform_data", key="tt-keys")
    print("tt_keys", tt_keys)

    for key in sp_keys:
        file = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
        data = json.loads(file)
        #check the response id and try to separate the stop_point_id and naptan_id 
        
        stmt = f"SELECT 1 FROM STOP_POINTS WHERE stop_point_id = '{data["stop_point_id"]}' LIMIT 1"
        cursor.execute(stmt)
        exists = cursor.fetchone()
        if exists:
            stmt = f"DELETE FROM STOP_POINTS WHERE stop_point_id = '{data["stop_point_id"]}'"
            cursor.execute(stmt)
        stmt = f"INSERT INTO STOP_POINTS (stop_point_id, naptan_id, common_name, lat, lon, modes, indicator,stop_type) SELECT"\
              f"'{data["stop_point_id"]}', '{data["naptan_id"]}', '{data["common_name"]}', {data["lat"]},{data["lon"]},{array_construct(data["modes"])}, '{data["indicator"]}', '{data["stop_type"]}'"
        cursor.execute(stmt)
    
    def get_day_id(day):
        stmt = f"SELECT id FROM service_days WHERE name = '{day}';" 
        ins = cursor.execute(stmt)
        rows = ins.fetchall()
        if rows:
            return rows[0][0]
        return None
    
    for key in tt_keys:
        file = s3_hook.read_key(key=key, bucket_name=BUCKET_NAME)
        data = json.loads(file)

        stmt = f"SELECT 1 FROM timetables WHERE line_id = '{data["line_id"]}' AND direction = '{data["direction"]}' AND from_stop_id = '{data["from_stop_id"]}' LIMIT 1"
        cursor.execute(stmt)
        exists = cursor.fetchone()
        if exists:
            stmt = f"DELETE FROM timetables WHERE line_id = '{data["line_id"]}' AND direction = '{data["direction"]}' AND from_stop_id = '{data["from_stop_id"]}'"
            cursor.execute(stmt)

        stmt = f"INSERT INTO timetables (line_id, direction, from_stop_id,stop_point_sequence) SELECT '{data["line_id"]}','{data["direction"]}','{data["from_stop_id"]}', {array_construct(data["stop_point_sequence"])};" 
        ins = cursor.execute(stmt)
        ins = cursor.execute("SELECT id FROM timetables ORDER BY id DESC limit 1;")
        rows = ins.fetchall()
        ttid = rows[0][0]
        print("Inserting into timetables with id: ",ttid)
        schedules = data["schedules"]
        for key, value in schedules.items():
            print("for key: ", key)
            day_of_week = key
            service_day_id = get_day_id(key)
            print("sched len: ",len(schedules[day_of_week]["schedule_time"]))
            print("ti len: ", len(schedules[day_of_week]["time_intervals"]))
            for schedule, time_interval in zip(schedules[day_of_week]["schedule_time"],schedules[day_of_week]["time_intervals"]):
                #check if the exact array is already present
                time_interval_str = ",".join([str(ti) for ti in time_interval])
                time = schedule.split("T")[-1]
                stmt = f"SELECT id FROM time_intervals WHERE ARRAY_TO_STRING(minutes, ',') = '{time_interval_str}';"
                ins = cursor.execute(stmt)
                rows = ins.fetchall()
                ti = rows[0][0] if rows else None
                if not ti:
                    stmt = f"INSERT INTO time_intervals (minutes) SELECT (ARRAY_CONSTRUCT({time_interval_str}));"
                    ins = cursor.execute(stmt)
                    rows = ins.fetchall()
                    ti = rows[0][0]
                
                stmt = f"INSERT INTO schedules (timetable_id,service_day_id,time_interval_id,time) VALUES ({ttid},{service_day_id},'{ti}','{time}')"
                cursor.execute(stmt)
                
            # for stop in schedules[key].keys():
            #     for time in schedules[key][stop]:
            #         t = time.split("T")[-1]
            #         stmt = f"INSERT INTO stop_times (timetable_id,service_day_id,stop_id,time) VALUES ({ttid},{service_day_id},'{stop}','{t}')"
            #         cursor.execute(stmt)

    logging.info(f"Successfully Inserted stop_points_metadata and timetable for {datetime.today()}")                

        
with DAG(
                dag_id= "TFL_DATA_DAG_V2",
                default_args=default_args,
                schedule_interval="@daily",
                start_date=days_ago(1)
            ) as dag:
    #task 1 pull the two static data
    task1 = PythonOperator(
        task_id="api_call_static_data",
        python_callable=get_api_data,
        provide_context=True,
    )
    task2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    task3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    task1 >> task2 >> task3
    