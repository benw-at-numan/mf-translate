with

source as (

    select * from {{ source('ecom', 'raw_delivery_people') }}
),

renamed as (

    select

        id as delivery_person_id,
        name as full_name

    from source

)

select * from renamed
