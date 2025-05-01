WITH airbnb AS (
    SELECT * FROM {{ ref('stg_airbnb_data') }}
),
weather AS (
    SELECT * FROM {{ ref('stg_weather_data') }}
),
events AS (
    SELECT * FROM {{ ref('stg_events_data') }}
)

SELECT 
    a.listing_id,
    a.city,
    a.price,
    a.availability_30,
    a.number_of_reviews,
    a.listing_date,
    w.temperature,
    w.weather_description,
    e.event_name,
    e.event_type
FROM airbnb a
LEFT JOIN weather w
  ON a.city = w.city_name AND a.listing_date = w.weather_date
LEFT JOIN events e
  ON a.city = e.city AND a.listing_date = e.event_date
