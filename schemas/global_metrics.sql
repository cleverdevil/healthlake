CREATE OR REPLACE VIEW daily_global_metrics AS

WITH t0 AS (
    SELECT 
        max(qty) AS daily_max_active_energy, 
        min(qty) AS daily_min_active_energy,
        avg(qty) AS daily_avg_active_energy
    FROM syncs WHERE name = 'active_energy'
), t1 AS (
    SELECT 
        max(qty) AS daily_max_apple_exercise_time,
        min(qty) AS daily_min_apple_exercise_time,
        avg(qty) AS daily_avg_apple_exercise_time
    FROM syncs WHERE name = 'apple_exercise_time'
), t2 AS (
    SELECT 
        max(qty) AS daily_max_apple_stand_hour,
        min(qty) AS daily_min_apple_stand_hour,
        avg(qty) AS daily_avg_apple_stand_hour
    FROM syncs WHERE name = 'apple_stand_hour' 
), t3 AS (
    SELECT 
        max(qty) AS daily_max_flights_climbed,
        min(qty) AS daily_min_flights_climbed,
        avg(qty) AS daily_avg_flights_climbed
    FROM syncs WHERE name = 'flights_climbed'
), t4 AS (
    SELECT 
        max(qty) AS daily_max_resting_heart_rate, 
        min(qty) AS daily_min_resting_heart_rate,
        avg(qty) AS daily_avg_resting_heart_rate
    FROM syncs WHERE name = 'resting_heart_rate'
), t5 AS (
    SELECT 
        max(syncs.max) AS daily_max_heart_rate_max,
        max(syncs.min) AS daily_max_heart_rate_min,
        max(syncs.avg) AS daily_max_heart_rate_avg,
        min(syncs.max) AS daily_min_heart_rate_max,
        min(syncs.min) AS daily_min_heart_rate_min,
        min(syncs.avg) AS daily_min_heart_rate_avg,
        avg(syncs.max) AS daily_avg_heart_rate_max,
        avg(syncs.min) AS daily_avg_heart_rate_min,
        avg(syncs.avg) AS daily_avg_heart_rate_avg
    FROM syncs WHERE name = 'heart_rate'
), t6 AS (
    SELECT
        max(qty) AS daily_max_step_count,
        min(qty) AS daily_min_step_count,
        avg(qty) AS daily_avg_step_count
    FROM syncs WHERE name = 'step_count'
), t7 AS (
    SELECT
        max(qty) AS daily_max_walking_running_distance,
        min(qty) AS daily_min_walking_running_distance,
        avg(qty) AS daily_avg_walking_running_distance
    FROM syncs WHERE name = 'walking_running_distance'
), t8 AS (
    SELECT
        max(qty) AS daily_max_walking_speed,
        min(qty) AS daily_min_walking_speed,
        avg(qty) AS daily_avg_walking_speed
    FROM syncs WHERE name = 'walking_speed'
), t9 AS (
    SELECT
        max(inbed) AS daily_max_sleep_analysis_inbed,
        min(inbed) AS daily_min_sleep_analysis_inbed,
        avg(inbed) AS daily_avg_sleep_analysis_inbed,
        max(asleep) AS daily_max_sleep_analysis_asleep,
        min(asleep) AS daily_min_sleep_analysis_asleep,
        avg(asleep) AS daily_avg_sleep_analysis_asleep
    FROM syncs WHERE name = 'sleep_analysis' AND asleep > 0 AND inbed > 0
)

SELECT * FROM t0, t1, t2, t3, t4, t5, t6, t7, t8, t9

