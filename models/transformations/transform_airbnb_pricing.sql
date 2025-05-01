WITH base AS (
  SELECT * FROM {{ ref('mart_airbnb_weather_events') }}
)

SELECT
  *,
  CASE
    WHEN price > 150 THEN 'High'
    WHEN price BETWEEN 75 AND 150 THEN 'Mid'
    ELSE 'Low'
  END AS price_category,
  
  CASE
    WHEN LOWER(weather_description) LIKE '%rain%' OR temperature < 10 THEN 'Bad'
    WHEN temperature BETWEEN 10 AND 25 THEN 'Moderate'
    ELSE 'Good'
  END AS weather_risk
FROM base
