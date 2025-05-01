WITH airbnb_data AS (
    SELECT * FROM {{ source('weather_dataset', 'barcelona_raw') }}
    UNION ALL
    SELECT * FROM {{ source('weather_dataset', 'brussels_raw') }}
    UNION ALL
    SELECT * FROM {{ source('weather_dataset', 'florence_raw') }}
    UNION ALL
    SELECT * FROM {{ source('weather_dataset', 'madrid_raw') }}
)

SELECT 
    id AS listing_id,
    city,
    host_name,
    price,
    availability_30,
    number_of_reviews,
    minimum_nights,
    room_type,
    listing_date
FROM airbnb_data
