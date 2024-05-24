{{
  config(
    materialized = 'view'
  )
}}

combined_tables as (
  select * from {{ ref('new_couchdb') }}
  union all
  select * from {{ ref('stable_couchdb') }}
)

select * from combined_tables
