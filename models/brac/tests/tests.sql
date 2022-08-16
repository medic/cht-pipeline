{{ config(materialized='table') }}

SELECT i.col1,
        i.col2
        FROM dblink('dbname=brac-ug-dev port=5432 hostaddr=localhost user=brac_ug_dblink password=Bc5Tty14vQaxAbw', 
                                'SELECT 
                                a.col1, 
                                a.col2
                                from 
                                (SELECT 
                                    col1,col2
                                 FROM 
                                    public.tableName) a
                                ') 
i (col1 integer, col2 varchar(20))