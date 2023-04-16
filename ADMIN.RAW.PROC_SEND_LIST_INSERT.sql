CREATE OR REPLACE PROCEDURE ADMIN.RAW.PROC_SEND_LIST_INSERT()
RETURNS TEXT
LANGUAGE SQL
AS
DECLARE 
i INT := 0;
RES resultset;
BEGIN

LET CALL_SEND_EMAIL TEXT:= '';
LET MSG TEXT:= '';


RES := ( SELECT * FROM ADMIN.RAW.TB_TASK_HISTORY ORDER BY TABLE_NAME, START_TIME );

LET C1 cursor FOR RES;
OPEN C1;
    FOR r IN C1 DO
        IF ( r.rn2 = 1 )
        THEN
            MSG := MSG || '\n';
        END IF;
        
        --MSG := MSG || 'TASK NAME: ' || r.TASK_NAME;
        MSG := MSG || 'DATA: ' || TO_CHAR(r.START_TIME::DATE);
        MSG := MSG || ' - TABLE: ' || r.TABLE_NAME;
        MSG := MSG || ' - INSERT: ' || r.ROWS_USED || ' rows.';
        MSG := MSG || ' - DURATION: ' || r.TOTAL_ELAPSED_TIME_SEG::NUMERIC(10,2)::TEXT || ' segs.';
        MSG := MSG || '\n';
        
    END for;
CLOSE C1;

MSG := 'Last 7 days of load data into tables:' || '\n' || MSG;

IF (LENGTH(MSG) > 0)
    THEN
        MSG := replace(MSG,'''','');      
        CALL_SEND_EMAIL := 'CALL SYSTEM$SEND_EMAIL(
                            ''DATA_SCIENCE.EMAIL_INTEGRATION_TASKS'',
                            ''email@dominio.com'',
                            ''Informativo: Contagem de insert nas tabelas.'',
                            ''' || MSG || '''
                        );';
    
        EXECUTE IMMEDIATE CALL_SEND_EMAIL;
    
    END IF;

RETURN MSG;

END
