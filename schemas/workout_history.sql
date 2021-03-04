CREATE OR REPLACE VIEW 
    workout_history 
AS SELECT
    date_parse(substr("start", 1, 19), '%Y-%m-%d %H:%i:%s') as start_datetime,
    date_parse(substr("end", 1, 19), '%Y-%m-%d %H:%i:%s') as end_datetime,
    '[' || array_join(transform(
        route, 
        x -> 
            '{"lat": ' || 
            cast(x.lat as varchar) || 
            ', "timestamp": "' || 
            x.timestamp || 
            '", "lon": ' || 
            cast(x.lon as varchar) || 
            ', "altitude": ' || 
            cast(x.altitude as varchar) ||
            '}'
    ), ', ') || ']' as route_json,
    *
FROM
  workouts


