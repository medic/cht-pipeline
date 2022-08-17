{{ config(materialized='raw_sql') }}

SELECT i.uuid
        FROM dblink('dbname=brac-ug-dev port=5432 host=localhost user=brac_ug_dblink password=Bc5Tty14vQaxAbw', 
                                'SELECT 
                                uuid 
                                FROM v1.couchdb
                                LIMIT 1
                                ') 
i (uuid TEXT)