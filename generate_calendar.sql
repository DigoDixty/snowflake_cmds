execute immediate $$
declare
    
    period string := 'MINUTE';
    dt_ini timestamp := '2020-01-01 00:00:00';
    dt_end timestamp := '2100-01-01 00:00:00';

    cmd string;
    i           INT := 1;
    i_end       INT := 1;
    count_loop  INT := 1;
    res resultset;
begin

    CREATE OR REPLACE TABLE period (day timestamp);
    
    cmd := 'SELECT DATEDIFF('||period||', '''|| dt_ini ||''', '''|| dt_end ||''') AS numbers;';
    res := (EXECUTE IMMEDIATE cmd);
    
    LET cur CURSOR FOR res;
    OPEN cur;
    FOR r IN cur DO
        i_end := r.numbers;
    END for;

    cmd := 'INSERT INTO period SELECT ''' || dt_ini || '''';
    EXECUTE IMMEDIATE (cmd);
    
    WHILE ( i < i_end ) DO
        count_loop := count_loop + 1;
        cmd := 'CREATE OR REPLACE TEMPORARY TABLE D AS SELECT DISTINCT DAY FROM period;'; 
        EXECUTE IMMEDIATE (cmd);

        cmd := 'INSERT INTO period SELECT DISTINCT DAY + INTERVAL '''|| i ||' '||UPPER(period)||''' FROM D;';
        EXECUTE IMMEDIATE (cmd);
        i := i * 2;
    END WHILE;

        cmd := 'DELETE FROM period WHERE DAY > '''|| dt_end ||''';';
        EXECUTE IMMEDIATE (cmd);

    RETURN 'Foram incluidos ' || i_end || ' registros em '|| count_loop || 'ciclos;';

end;
$$;

SELECT EXTRACT(YEAR FROM DAY), COUNT(1) 
FROM period
GROUP BY 1
ORDER BY 1 DESC;
