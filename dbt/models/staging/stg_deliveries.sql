with

source as (

    select * from {{ source('ecom', 'raw_deliveries') }}

    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line
    -- where timestamp <= {{ var('truncate_timespan_to') }}
),

renamed as (

    select

        id as delivery_id,
        order_id,
        timestamp as delivered_at,
        person_id as delivery_person_id,
        rating as delivery_rating

    from source

)

select * from renamed
