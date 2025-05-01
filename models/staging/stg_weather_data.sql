SELECT 
    city_name,
    temperature,
    humidity,
    weather_description,
    DATE(timestamp) AS weather_date
FROM
    {{ source('weather_dataset', 'live_weather_data') }}
