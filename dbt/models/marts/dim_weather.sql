select
    {{ dbt_utils.generate_surrogate_key(['reference_date', 'state_code']) }} as weather_id,
    reference_date,
    state_code,
    precipitation_mm,
    temp_max_c,
    temp_min_c,
    humidity_pct,
    is_rainy_day,
    rain_category
from {{ ref('stg_weather') }}
