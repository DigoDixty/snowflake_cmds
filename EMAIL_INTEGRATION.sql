CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_INTEGRATION_TASKS
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('rodrigo.cardoso@triggo.ai')
;

GRANT USAGE ON INTEGRATION EMAIL_INTEGRATION TO ROLE ACCOUNTADMIN;


CREATE OR REPLACE TASK TESTES.PUBLIC.TESTE_EMAIL_CURSOR
	WAREHOUSE = COMPUTE_WH
    AS 
	 CALL TESTES.PUBLIC.TESTE_EMAIL_CURSOR()
;


SELECT * 
FROM snowflake.account_usage.task_history
WHERE STATE = 'FAILED'
;

SELECT NAME, ERROR_CODE, ERROR_MESSAGE, *
FROM snowflake.account_usage.task_history
WHERE STATE = 'FAILED'
ORDER BY query_start_time DESC
LIMIT 5
;

SELECT NAME, ERROR_CODE, ERROR_MESSAGE, QUERY_START_TIME 
FROM snowflake.account_usage.task_history
WHERE STATE = 'FAILED'
AND scheduled_time > dateadd(DAY,-1,current_timestamp())
AND query_start_time > dateadd(DAY,-1,current_timestamp())
ORDER BY QUERY_START_TIME DESC
;


CALL SYSTEM$SEND_EMAIL(
    'EMAIL_INTEGRATION',
    'rodrigo.cardoso@triggo.ai',
    'Email Alert: Task has failed.',
    'Task\n');


--CREATE OR REPLACE PROCEDURE TESTE_EMAIL_CURSOR() RETURNS TEXT LANGUAGE SQL AS

EXECUTE IMMEDIATE
$$
BEGIN
    
    LET CALL_SEND_EMAIL TEXT:= '';
    LET MSG TEXT:= '';
        
    LET C1 cursor FOR   SELECT NAME, ERROR_CODE, ERROR_MESSAGE, QUERY_START_TIME 
                        FROM snowflake.account_usage.task_history
                        WHERE STATE = 'FAILED'
                        AND scheduled_time > dateadd(DAY,-1,current_timestamp())
                        AND query_start_time > dateadd(DAY,-1,current_timestamp())
                        ORDER BY QUERY_START_TIME DESC
                        ;
    OPEN C1;
        FOR r IN C1 DO
            MSG := MSG || 'EXECUTION TIME: ' || r.QUERY_START_TIME || '\n';
            MSG := MSG || 'TASK: ' || r.NAME || '\n';
            MSG := MSG || 'ERROR CODE: ' || r.ERROR_CODE || '\n';
            MSG := MSG ||  r.ERROR_MESSAGE || '\n' || '\n';
        END for;
    CLOSE C1;

    IF (LENGTH(MSG) > 0)
    THEN
        MSG := REPLACE(MSG,'''','');
        CALL_SEND_EMAIL := 'CALL SYSTEM$SEND_EMAIL(
                            ''EMAIL_INTEGRATION'',
                            ''rodrigo.cardoso@triggo.ai'',
                            ''Email Alert: Task has failed.'',
                            ''' || MSG || '''
                        );';
    
        EXECUTE IMMEDIATE CALL_SEND_EMAIL;
    
    END IF;
    
    RETURN 'Success';    

    END;
$$
;
