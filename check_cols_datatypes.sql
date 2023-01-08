--- After load table, use infer_schema to get columns to check load data on target table.

EXECUTE IMMEDIATE $$
DECLARE 
RES resultset;
QUERY STRING;

schemaname := 'PUBLIC';
val_stage  := 'PARQUET_STAGE';
table_name := 'TABLE';
file_name  := 'directory/file.parquet';
file_type  := 'parquet_format';

CMD_to_replace := '     INSERT INTO CHECK_COLUMNS_TABLE
             SELECT  ''col_name'' as COLUMN_NAME,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name is null) AS COL_NULL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name is not null) AS NOT_NULL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where trim(col_name) <> '''''''') AS NOT_EMPTY,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name LIKE ''%-%T%:%:%.%'') AS TIMESTAMP,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where is_real(to_variant(col_name)) = true) AS ONLY_NUMBERS,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name like ''%.%'') AS HAS_DECIMAL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' ) AS full_table; ';

CMD := CMD_to_replace;

BEGIN

QUERY := 'SELECT COLUMN_NAME AS COLUMNS
          --SELECT COLUMN_NAME || '' '' || TYPE || '','' AS COLUMNS
          FROM TABLE (infer_schema(location=>''@' || val_stage || '/' || file_name || ''', 
          file_format=>'''|| file_type ||'''))
          ORDER BY 1
          --LIMIT 5
         ;';
         
RES := ( EXECUTE IMMEDIATE QUERY );

QUERY := 'CREATE OR REPLACE TEMPORARY TABLE CHECK_COLUMNS_TABLE
            (
            COLUMN_NAME STRING,
            COL_NULL INT,
            NOT_NULL INT,
            NOT_EMPTY INT,
            TIMESTAMP INT,
            ONLY_NUMBERS INT,
            HAS_DECIMAL INT,
            FULL_TABLE INT
            );
        ';
        
EXECUTE IMMEDIATE (QUERY);

LET CUR CURSOR FOR RES;
OPEN CUR;
    FOR row_variable IN CUR DO
        -- CMD := CMD || CHAR(10) REPLACE(CMD,'col_name',row_variable.COLUMNS);     
        CMD := REPLACE(CMD,'col_name',row_variable.COLUMNS);  
        EXECUTE IMMEDIATE (CMD);
        CMD := CMD_to_replace;
        
        
    END FOR;
CMD := SUBSTRING(CMD,0,LENGTH(CMD)-1);

RETURN 'SUCESS';
END;
$$;


SELECT  COLUMN_NAME,
        CASE WHEN timestamp = not_empty AND COL_NULL < FULL_TABLE THEN 'TIMESTAMP' 
             WHEN ONLY_NUMBERS > 0      AND HAS_DECIMAL > 0 THEN 'REAL'
             WHEN ONLY_NUMBERS > 0      AND HAS_DECIMAL = 0 THEN 'INTEGER'
             ELSE 'STRING' END TYPE
,* 
FROM CHECK_COLUMNS_TABLE;


