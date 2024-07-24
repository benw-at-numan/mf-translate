select
    delivery_person_id,
    full_name

from {{ ref('stg_delivery_people') }}