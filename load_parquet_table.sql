CREATE OR REPLACE PROCEDURE public.load_parquet_table(schemaname string , val_stage string, table_name string, file_type string)
RETURNS string
LANGUAGE SQL
AS $$
DECLARE
cmd string := '';
query string := '';
res resultset;
i int := 0;
col string := '';
val_stage2 := '@PUBLIC.' ||val_stage || '/';
BEGIN
query :=  'SELECT EXPRESSION AS COLUMNS 
           FROM TABLE (infer_schema(location=>''' || val_stage2 || table_name || '/LOAD00000001.parquet'', 
           file_format=>'''|| file_type ||'''));';
res := (EXECUTE IMMEDIATE query);
let cur cursor for res;
OPEN cur;
    FOR row_variable IN cur DO
        i := i + 1;
        col := 'col' ||  i::string;
        CMD := CMD || ', ' || row_variable.COLUMNS || ' AS ' || col;
    END for;
CMD := SUBSTRING(CMD,2,LENGTH(CMD)-1);
CMD := ' COPY INTO ' || schemaname ||'.RAW_' || table_name || 
       ' FROM
        ( SELECT '||CMD||' 
           FROM '|| val_stage2 || table_name || '/LOAD00000001.parquet)
       pattern=''.*''
       ';
EXECUTE IMMEDIATE CMD;
END
$$;
