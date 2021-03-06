CREATE TABLE IF NOT EXISTS public.lineups
(
    game_id bigint,
    team_id bigint,
    player_id bigint,
    startTime timestamp with time zone,
    endedTime timestamp with time zone,
    awayTeam_id bigint,
    awayTeam_abbreviation text,
    homeTeam_id bigint,
    homeTeam_abbreviation text,
    venue_id bigint,
    venue_name text,
    venueAllegiance text,
    scheduleStatus text,
    originalStartTime text,
    delayedOrPostponedReason text,
    playedStatus text,
    attendance double precision,
    weather_type text,
    weather_description text,
    weather_wind_speed_milesPerHour bigint,
    weather_wind_speed_kilometersPerHour bigint,
    weather_wind_direction_degrees bigint,
    weather_wind_direction_label text,
    weather_temperature_fahrenheit bigint,
    weather_temperature_celsius bigint,
    weather_precipitation_type text,
    weather_precipitation_percent text,
    weather_precipitation_amount_millimeters double precision,
    weather_precipitation_amount_centimeters text,
    weather_precipitation_amount_inches text,
    weather_precipitation_amount_feet text,
    weather_humidityPercent bigint,
    team_abbreviation text,
    type text,
    position text,
    player_firstName text,
    player_lastName text,
    player_position text,
    player_jerseyNumber text
);