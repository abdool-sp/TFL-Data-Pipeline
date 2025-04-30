ALTER SESSION SET TIMEZONE = 'Europe/London';
CREATE DATABASE TFL_OPEN_DATA;
use DATABASE TFL_OPEN_DATA;

CREATE SCHEMA tfl_data;
use schema tfl_data;

--arrival table
CREATE OR REPLACE TABLE arrivals (
    id                    STRING PRIMARY KEY,
    line_id               STRING,
    vehicle_id            STRING,
    naptan_id             STRING,
    stop_point_id         STRING,
    station_name          STRING,
    direction             STRING,
    expected_arrival      TIMESTAMP_TZ,
    destination_name      STRING,
    platform_name         STRING,
    current_location      STRING,
    bearing               STRING,
    time_to_station       INTEGER,
    mode_name             STRING,
    created_at            TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE stop_points (
    stop_point_id         STRING PRIMARY KEY,
    naptan_id             STRING,
    common_name           STRING,
    lat                   FLOAT,
    lon                   FLOAT,
    stop_type             STRING,
    modes                 ARRAY,
    lines                 ARRAY,
    parent_id             STRING,
    zone                  STRING,
    towards               STRING,
    indicator             STRING,
    created_at            TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE service_days (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(20) UNIQUE NOT NULL  
);

INSERT INTO service_days (name) VALUES 
('Sunday'),
('Monday'),
('Tuesday'),
('Wednesday'),
('Thursday'),
('Friday'),
('Saturday'),
('Monday to Thursday');


CREATE OR REPLACE TABLE timetables (
    id INT AUTOINCREMENT PRIMARY KEY,
    line_id VARCHAR(50) NOT NULL,        
    direction VARCHAR(20),               
    from_stop_id VARCHAR(50),            
    stop_point_sequence ARRAY,
    created_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE time_intervals (  
    id INT IDENTITY(1,1) PRIMARY KEY,
    minutes        ARRAY  
);


CREATE OR REPLACE TABLE schedules (
    id INT IDENTITY(1,1) PRIMARY KEY,
    timetable_id INTEGER REFERENCES timetables(id) ON DELETE CASCADE,
    service_day_id INTEGER REFERENCES service_days(id) ON DELETE CASCADE,
    time_interval_id INTEGER REFERENCES time_intervals(id) ON DELETE CASCADE,
    time TIME NOT NULL                    
);


CREATE OR REPLACE TABLE disruptions (
    disruption_id         STRING PRIMARY KEY,
    category              STRING,
    type                  STRING,
    description           STRING,
    affected_routes       ARRAY,
    affected_stops        ARRAY,
    is_planned            BOOLEAN,
    is_active             Boolean DEFAULT FALSE,
    start_time            TIMESTAMP_TZ NULL,
    end_time              TIMESTAMP_TZ NULL,
    last_updated          TIMESTAMP_TZ NULL,
    created_at            TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);



