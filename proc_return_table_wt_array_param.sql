CREATE OR REPLACE PROCEDURE PR_LETTER (DT_INI STRING, DT_END STRING, ABC STRING)
RETURNS TABLE()
LANGUAGE SQL
AS
DECLARE 
r RESULTSET;
BEGIN

CREATE OR REPLACE TEMPORARY TABLE TEST
(
DATA_PROC STRING,
LETTER STRING
);

INSERT INTO TEST
VALUES ('3/1/2023 12:07:39 AM', 'a'),('3/1/2023 10:00:23 PM', 'b'),('3/1/2023 7:00:23 PM', 'c')
;

r := (execute immediate ('SELECT * 
                          FROM TEST
                          WHERE 1 = 1 
                          AND DATE(DATA_PROC,''DD/MM/YYYY HH:MI:SS PM'') BETWEEN '''|| DT_INI || ''' AND ''' || DT_END || '''
                          AND LETTER IN
                          (
                          SELECT REPLACE(value,''"'','''') AS LETTER 
                          FROM TABLE(FLATTEN(input => SELECT STRTOK_TO_ARRAY(''' || ABC || ''', ''.''))) f);
                          '));
RETURN TABLE(r);
END
;
-------

CALL PR_LETTER ('2023-01-01','2023-01-05','a.b.c.d')
;
