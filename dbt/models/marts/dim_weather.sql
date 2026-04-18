select
    {{ dbt_utils.generate_surrogate_key(['reference_date', 'state_code']) }} as weather_id,
    reference_date,
    state_code,
    max(precipitation_mm) as precipitation_mm,
    max(temp_max_c)       as temp_max_c,
    min(temp_min_c)       as temp_min_c,
    max(humidity_pct)     as humidity_pct,
    max(is_rainy_day)     as is_rainy_day,
    max(rain_category)    as rain_category
from {{ ref('stg_weather') }}
group by reference_date, state_code
