{{ config(materialized='table') }}

select
    null::timestamp as saved_timestamp,
    null::text as _id,
    null::boolean as _deleted,
    null::jsonb as doc
