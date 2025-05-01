SELECT 
    city,
    event_name,
    event_type,
    CAST(event_date AS DATE) AS event_date
FROM 
    {{ source('weather_dataset', 'events_raw') }}
