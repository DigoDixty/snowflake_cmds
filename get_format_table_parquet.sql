create or replace procedure public.get_parquet_table(schemaname string , val_stage string, table_name string, file_type string)
returns string
language sql
as $$
DECLARE
cmd string := '';
query string := '';
res resultset;

val_stage2 := '@PUBLIC.' ||val_stage || '/';

BEGIN
query :=  'SELECT COLUMN_NAME || '' '' || TYPE || '','' AS COLUMNS 
           FROM TABLE (infer_schema(location=>''' || val_stage2 || table_name || '/LOAD00000001.parquet'', 
           file_format=>'''|| file_type ||'''));';

res := (EXECUTE IMMEDIATE query);

let cur cursor for res;

OPEN cur;
    FOR row_variable IN cur DO
        CMD := CMD || row_variable.COLUMNS;
    END for;

CMD := SUBSTRING(CMD,0,LENGTH(CMD)-1);

CMD := 'CREATE OR REPLACE TABLE '||schemaname||'.RAW_' || table_name || ' ('||CMD||');';

return CMD;

END
$$;
