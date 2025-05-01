WITH base AS (
    SELECT * FROM {{ ref('mart_airbnb_weather_events') }}
)

SELECT
    listing_id,
    city,
    listing_date,
    temperature,
    weather_description,
    CASE
        WHEN LOWER(weather_description) LIKE '%storm%' THEN 'High Risk'
        WHEN LOWER(weather_description) LIKE '%rain%' THEN 'Moderate Risk'
        WHEN temperature < 5 THEN 'Cold Risk'
        ELSE 'Low Risk'
    END AS weather_risk_category
FROM base
