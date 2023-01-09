EXECUTE IMMEDIATE $$
DECLARE 
RES resultset;
QUERY STRING;

schemaname := 'schema';
val_stage  := 'PARQUET_STAGE';
table_name := 'TABLENAME';
file_name  := 'directory/file.parquet';
file_type  := 'parquet_format';

CMD_to_replace := '     INSERT INTO CHECK_COLUMNS_TABLE
             SELECT  ''col_name'' as COLUMN_NAME,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name is null) AS COL_NULL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name is not null) AS NOT_NULL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where trim(col_name) <> '''''''') AS NOT_EMPTY,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name LIKE ''%-%-%T%:%:%.%'' or col_name LIKE ''%-%-%:%:%.%'' or col_name LIKE ''%-%-%:%:%'' ) AS COL_TIMESTAMP,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name LIKE ''____-__-__'') AS COL_DATE,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where is_real(to_variant(col_name)) = true) AS ONLY_NUMBERS,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name like ''%.%'') AS HAS_DECIMAL,
            (SELECT count(1) from ' || schemaname || '.' || table_name || ' where col_name in (''0'',''1'') ) AS COL_BOOLEAN,
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
            COL_TIMESTAMP INT,
            COL_DATE INT,
            ONLY_NUMBERS INT,
            HAS_DECIMAL INT,
            COL_BOOLEAN INT,
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

RETURN QUERY;
END;
$$;
