{{ config(materialized='table') }}

{{
    dbt_date.get_date_dimension(
        start_date="2016-01-01",
        end_date="2018-12-31"
    )
}}
