SELECT
    couchdb.doc ->> '_id'::text AS uuid,
    couchdb.doc #>> '{survey,solar_light}'::text[] AS solar_light,
    couchdb.doc #>> '{survey,water_filter}'::text[] AS water_filter,
    couchdb.doc #>> '{survey,children_under_5}'::text[] AS children_under_5,
    couchdb.doc #>> '{survey,improved_cook_stove}'::text[] AS improved_cook_stove
FROM
    {{ ref("couchdb") }}
WHERE
    (couchdb.doc ->> 'type') = 'clinic'