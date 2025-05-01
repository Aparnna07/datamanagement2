WITH base AS (
    SELECT * FROM {{ ref('mart_airbnb_weather_events') }}
)

SELECT
    city,
    event_name,
    event_type,
    listing_date AS event_date,
    COUNT(DISTINCT listing_id) AS listings_available,
    AVG(price) AS avg_price,
    AVG(availability_30) AS avg_availability
FROM base
WHERE event_name IS NOT NULL
GROUP BY city, event_name, event_type, listing_date
ORDER BY city, listing_date
