with stations as (
    select
        codigo                                    as station_code,
        nome                                      as station_name,
        -- El nombre viene como "CIDADE - UF", extraemos el estado
        trim(split_part(nome, ' - ', -1))         as state_code,
        latitude,
        longitude,
        altitude
    from {{ source('br_weather', 'weather_stations_codes') }}
    where nome is not null
),

observations as (
    select
        strptime(data, '%d/%m/%Y')::date  as reference_date,
        cast(estacao as integer)           as station_code,
        precipitacao                       as precipitation_mm,
        tempmaxima                         as temp_max_c,
        tempminima                         as temp_min_c,
        umidade_relativa_media             as humidity_pct,
        velocidade_do_vento_media          as wind_speed_avg,
        nebulosidade                       as cloudiness,
        pressaoatmestacao                  as pressure_station

    from {{ source('br_weather', 'conventional_weather_stations') }}
    where data is not null
      and estacao is not null
),

joined as (
    select
        o.reference_date,
        s.state_code,
        s.station_name,
        s.station_code,
        s.latitude,
        s.longitude,
        coalesce(o.precipitation_mm, 0)   as precipitation_mm,
        o.temp_max_c,
        o.temp_min_c,
        o.humidity_pct,
        o.wind_speed_avg,
        o.cloudiness,
        o.pressure_station,

        -- flags derivados
        case
            when coalesce(o.precipitation_mm, 0) > 10 then true
            else false
        end as is_rainy_day,

        case
            when coalesce(o.precipitation_mm, 0) = 0   then 'dry'
            when coalesce(o.precipitation_mm, 0) <= 10  then 'light'
            when coalesce(o.precipitation_mm, 0) <= 50  then 'moderate'
            else 'heavy'
        end as rain_category

    from observations o
    inner join stations s on o.station_code = s.station_code
    where s.state_code is not null
      and length(trim(s.state_code)) = 2   -- filtra filas con nombre mal formado
)

select * from joined
