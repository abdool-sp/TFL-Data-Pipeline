ALTER SESSION SET TIMEZONE = 'Europe/London';
use DATABASE TFL_OPEN_DATA;

use schema tfl_data;


CREATE OR REPLACE VIEW busiest_stops_7days AS 
SELECT
    a.stop_point_id,
    s.common_name AS stop_name,
    COUNT(*) AS total_arrivals_7d
FROM
    tfl_data.arrivals a
JOIN
    tfl_data.stop_points s ON a.stop_point_id = s.stop_point_id
WHERE
    a.expected_arrival >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY
    a.stop_point_id, s.common_name
ORDER BY
    total_arrivals_7d DESC
LIMIT 50;

CREATE OR REPLACE VIEW busiest_stops_1hour AS 
SELECT
    a.stop_point_id,
    s.common_name,
    COUNT(*) AS arrivals_count
FROM
    tfl_data.arrivals a
JOIN
    tfl_data.stop_points s ON a.stop_point_id = s.stop_point_id
WHERE
    expected_arrival >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
GROUP BY
    a.stop_point_id, s.common_name
ORDER BY
    arrivals_count DESC
LIMIT 10;


CREATE OR REPLACE VIEW busiest_service_days AS
SELECT
    tt.line_id,
    service_day.name AS day_of_week,
    COUNT(*) AS scheduled_services
FROM
    tfl_data.timetables tt
JOIN tfl_data.schedules s ON s.timetable_id = tt.id 
JOIN tfl_data.service_days service_day ON s.service_day_id = service_day.id
GROUP BY
    line_id, day_of_week
ORDER BY
    scheduled_services DESC;

CREATE OR REPLACE VIEW next_arrivals AS 
WITH next_arrivals AS (
    SELECT
        stop_point_id,
        line_id,
        destination_name,
        expected_arrival,
        ROW_NUMBER() OVER (
            PARTITION BY stop_point_id
            ORDER BY expected_arrival ASC
        ) AS rn
    FROM
        tfl_data.arrivals
    WHERE
        expected_arrival >= CURRENT_TIMESTAMP()
)
SELECT
    stop_point_id,
    line_id,
    destination_name,
    expected_arrival
FROM
    next_arrivals
WHERE
    rn = 1
ORDER BY
    expected_arrival ASC;