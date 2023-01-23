create or replace procedure get_avro_table(table_name string)
returns string
language sql
as $$
DECLARE
cmd string := '';
query string := '';
res resultset;

BEGIN
query :=  'SELECT COLUMN_NAME || '' '' || TYPE || '','' AS COLUMNS 
           FROM TABLE (infer_schema(location=>''@STAGE/' || table_name || '.avro'', 
           file_format=>''LOAD_AVRO_FILES''));';

res := (EXECUTE IMMEDIATE query);

let cur cursor for res;

OPEN cur;
    FOR row_variable IN cur DO
        CMD := CMD || row_variable.COLUMNS;
    END for;

CMD := SUBSTRING(CMD,0,LENGTH(CMD)-1);

CMD := 'CREATE TABLE IF NOT EXISTS RAW_DATA.TESTE_' || table_name || ' ('||CMD||');';

return CMD;

END
$$;

