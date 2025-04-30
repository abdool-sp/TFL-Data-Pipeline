import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import os


env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
role = os.getenv("SNOWFLAKE_ROLE")

con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse = warehouse,
    database=database,
    schema=schema,
    role=role,
    session_parameters={
        'QUERY_TAG': 'TflOpenDATAKafkaConsumer',
    }
)
print(f"Connected as {user} to {account}")
con.cursor().execute("ALTER SESSION SET TIMEZONE = 'Europe/London';")
# con.cursor().execute("USE DATABASE TFL_OPEN_DATA;")
# con.cursor().execute("USE SCHEMA tfl_data;")

def array_construct(array):
    new_array = []
    b = "ARRAY_CONSTRUCT("
    for arr in array:
        new_array.append("'" + arr + "'")
    d = ",".join(new_array)
    return b + d + ")"

def insert_arrival(cleaned_data):
    for data in cleaned_data[:1]:
        insert_query = f"""
            INSERT INTO arrivals (
                id,
                line_id, 
                vehicle_id, 
                naptan_id, 
                stop_point_id, 
                direction, 
                expected_arrival, 
                destination_name, 
                platform_name, 
                current_location, 
                bearing, 
                time_to_station, 
                mode_name
            )
            VALUES (
                '{data.get("id")}',
                '{data["line_id"]}', 
                '{data["vehicle_id"]}', 
                '{data["naptan_id"]}', 
                '{data["station_name"]}', 
                '{data["direction"]}', 
                '{data["expected_arrival"]}', 
                '{data["destination_name"]}', 
                '{data["platform_name"]}', 
                '{data["current_location"]}', 
                '{data["bearing"]}', 
                '{data["time_to_station"]}', 
                '{data["mode_name"]}'
            );
        """
        con.cursor().execute(insert_query)
    print("arrival data inserted")

def test():
    return "yes"

def insert_arrival_(cleaned_data):
    #print(len(cleaned_data))
    for data in cleaned_data:
        insert_query = f"INSERT INTO arrivals (id, line_id, vehicle_id, naptan_id, stop_point_id,direction, expected_arrival, destination_name, platform_name, current_location, bearing, time_to_station, mode_name, station_name) VALUES ('{data.get('id')}', '{data['line_id']}', '{data['vehicle_id']}', '{data['naptan_id']}', '{data['naptan_id']}','{data['direction']}', '{data['expected_arrival']}', '{data['destination_name']}', '{data['platform_name']}', '{data['current_location']}', '{data['bearing']}', '{data['time_to_station']}', '{data['mode_name']}', '{data['station_name']}');"
        try:
            con.cursor().execute(insert_query)
        except:
            print("skipped")
            continue

def insert_disruption_data(data):
    stmt = f"INSERT INTO diruptions (id, line_id, category, type, description, is_planned, affected_routes, affected_stops, start_time" \
                                    f"end_time, last_update) SELECT ('{data["id"]}', '{data["line_id"]}', '{data['category']}', '{data['type']}'," \
                                    f"'{data["description"]}', '{data["is_planned"]}', {array_construct(data["affected_routes"])}, {array_construct(data["affected_stops"])}," \
                                    f"'{data["start_time"]}', '{data["end_time"]}', '{data["last_update"]}')"  
    ins = con.cursor().execute(stmt)
    for row in ins.fetchall():
        print(row)

