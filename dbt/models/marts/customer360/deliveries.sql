select
    delivery_id,
    order_id,
    delivered_at,
    delivery_person_id,
    delivery_rating

from {{ ref('stg_deliveries') }}