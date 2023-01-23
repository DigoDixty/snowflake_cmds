CREATE OR REPLACE PROCEDURE public.create_tasks_parquet(schemaname string , table_name string, stage string, v_warehouse string, file_name string, file_format string)
RETURNS string
LANGUAGE SQL
AS $$
DECLARE
query string := '';
BEGIN
query1 :=  ' CREATE OR REPLACE TASK '|| schemaname || '.' || table_name || '_TASK 
            WAREHOUSE='|| v_warehouse ||' SCHEDULE = ''USING CRON 0 3 * * * America/Sao_Paulo''
            USER_TASK_TIMEOUT_MS = 86400000
            AS CALL load_parquet_table('''|| schemaname ||''', '''||stage||''', '''||table_name||''', '''||file_name||''', '''||file_format||''');';

-- EXECUTE IMMEDIATE query1;

query2 := 'ALTER TASK '|| schemaname || '.' || table_name || '_TASK RESUME;';

-- EXECUTE IMMEDIATE query2;
RETURN query1;

END
$$;
