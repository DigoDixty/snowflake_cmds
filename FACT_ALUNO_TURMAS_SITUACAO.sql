CREATE FILE FORMAT IF NOT EXISTS parquet_format
type = 'parquet';

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::952166119828:role/snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ( 's3://advanced-analytics-dw/' )
;

DESC INTEGRATION S3_INT
;

CREATE STAGE IF NOT EXISTS parquet_stage 
  file_format = parquet_format
  url = 's3://advanced-analytics-dw/'
  storage_integration = S3_INT;

LIST @parquet_stage/lyceum/aluno_turmas_situacao;

call public.get_parquet_table('public' , 'parquet_stage', 'ug_aluno_turmas_situacao', 'lyceum/aluno_turmas_situacao/aluno_turmas_situacao.parquet', 'parquet_format');

CREATE OR REPLACE TABLE LAKE.UG.FACT_ALUNO_TURMAS_SITUACAO (
aluno TEXT,
ano TEXT,
ano_ingresso TEXT,
celular TEXT,
curriculo TEXT,
curso TEXT,
data_execucao TEXT,
data_onboarding TEXT,
ddd_fone TEXT,
ddd_fone_celular TEXT,
desc_turno TEXT,
disciplina TEXT,
dt_confirmacao_pre_mtr TEXT,
dt_matricula TEXT,
e_mail TEXT,
e_mail_interno TEXT,
inadimplente TEXT,
last_load INTEGER,
mailbox TEXT,
nome_aluno TEXT,
nome_curso TEXT,
nome_disciplina TEXT,
nome_ue TEXT,
nome_uf TEXT,
onboarding TEXT,
periodo TEXT,
pre_matr_alocada TEXT,
pre_matr_confirmada TEXT,
sem_ingresso TEXT,
serie TEXT,
sit_detalhe TEXT,
sit_matricula TEXT,
telefone TEXT,
tipo_curso TEXT,
turma TEXT,
turma_pref TEXT,
turno TEXT,
unidade_ensino TEXT,
unidade_fisica TEXT);

call public.load_parquet_table('public' , 'parquet_stage', 'ug_aluno_turmas_situacao', 'lyceum/aluno_turmas_situacao/aluno_turmas_situacao.parquet', 'parquet_format');

    
CREATE OR REPLACE TASK LAKE.UG.FACT_ALUNO_TURMAS_SITUACAO
	WAREHOUSE = DEV_DATA_SCIENCE
	AFTER LAKE.UG.DADOS_ALUNO
	AS   
     COPY INTO LAKE.UG.FACT_ALUNO_TURMAS_SITUACAO FROM
        ( SELECT  
$1:aluno::TEXT                      AS col1, 
$1:ano::TEXT                        AS col2, 
$1:ano_ingresso::TEXT               AS col3, 
$1:celular::TEXT                    AS col4, 
$1:curriculo::TEXT                  AS col5, 
$1:curso::TEXT                      AS col6, 
$1:data_execucao::TEXT              AS col7, 
$1:data_onboarding::TEXT            AS col8, 
$1:ddd_fone::TEXT                   AS col9, 
$1:ddd_fone_celular::TEXT           AS col10, 
$1:desc_turno::TEXT                 AS col11, 
$1:disciplina::TEXT                 AS col12, 
$1:dt_confirmacao_pre_mtr::TEXT     AS col13, 
$1:dt_matricula::TEXT               AS col14, 
$1:e_mail::TEXT                     AS col15, 
$1:e_mail_interno::TEXT             AS col16, 
$1:inadimplente::TEXT               AS col17, 
$1:last_load::INTEGER               AS col18, 
$1:mailbox::TEXT                    AS col19, 
$1:nome_aluno::TEXT                 AS col20, 
$1:nome_curso::TEXT                 AS col21, 
$1:nome_disciplina::TEXT            AS col22, 
$1:nome_ue::TEXT                    AS col23, 
$1:nome_uf::TEXT                    AS col24, 
$1:onboarding::TEXT                 AS col25, 
$1:periodo::TEXT                    AS col26, 
$1:pre_matr_alocada::TEXT           AS col27, 
$1:pre_matr_confirmada::TEXT        AS col28, 
$1:sem_ingresso::TEXT               AS col29, 
$1:serie::TEXT                      AS col30, 
$1:sit_detalhe::TEXT                AS col31, 
$1:sit_matricula::TEXT              AS col32, 
$1:telefone::TEXT                   AS col33, 
$1:tipo_curso::TEXT                 AS col34, 
$1:turma::TEXT                      AS col35, 
$1:turma_pref::TEXT                 AS col36, 
$1:turno::TEXT                      AS col37, 
$1:unidade_ensino::TEXT             AS col38, 
$1:unidade_fisica::TEXT             AS col39 
           FROM @ug.parquet_stage/lyceum/aluno_turmas_situacao/aluno_turmas_situacao.parquet)
       pattern='.*'
;        

ALTER TASK LAKE.UG.FACT_ALUNO_TURMAS_SITUACAO RESUME
;

ALTER TASK Lake.UG.DADOS_ALUNO SUSPEND
;

SHOW TASKS;
