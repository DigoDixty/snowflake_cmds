create or replace procedure public.get_parquet_table(schemaname string , val_stage string, table_name string, file_name string, file_type string)
returns string
language sql
as $$
DECLARE
cmd string := '';
query string := '';
res resultset;

BEGIN
query :=  'SELECT COLUMN_NAME || '' '' || TYPE || '','' AS COLUMNS 
           FROM TABLE (infer_schema(location=>''@PUBLIC.' ||val_stage || '/' || file_name || ''', 
           file_format=>'''|| file_type ||'''));';

res := (EXECUTE IMMEDIATE query);

let cur cursor for res;

OPEN cur;
    FOR row_variable IN cur DO
        CMD := CMD || CHAR(10) || row_variable.COLUMNS;
    END for;

CMD := SUBSTRING(CMD,0,LENGTH(CMD)-1);

CMD := 'CREATE OR REPLACE TABLE '||schemaname||'.RAW_' || table_name || ' ('||CMD||');';

--EXECUTE IMMEDIATE CMD;
RETURN CMD;

END
$$;

/*
call public.get_parquet_table('public' , 'st_teste', 'MARD', 'MARD.parquet', 'file_format_parquet');
*/
