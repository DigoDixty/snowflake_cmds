create or replace role datahub_role;

SET your_warehouse = 'RAW_DATA';

-- Grant access to a warehouse to run queries to view metadata
grant operate, usage on warehouse $your_warehouse to role datahub_role;

-- Grant access to view database and schema in which your tables/views exist
grant usage on DATABASE $your_warehouse to role datahub_role;
grant usage on all schemas in database $your_warehouse to role datahub_role;
grant usage on future schemas in database $your_warehouse to role datahub_role;

-- If you are NOT using Snowflake Profiling feature: Grant references privileges to your tables and views 
grant references on all tables in database $your_warehouse to role datahub_role;
grant references on future tables in database $your_warehouse to role datahub_role;
grant references on all external tables in database $your_warehouse to role datahub_role;
grant references on future external tables in database $your_warehouse to role datahub_role;
grant references on all views in database $your_warehouse to role datahub_role;
grant references on future views in database $your_warehouse to role datahub_role;

-- If you ARE using Snowflake Profiling feature: Grant select privileges to your tables and views 
grant select on all tables in database $your_warehouse to role datahub_role;
grant select on future tables in database $your_warehouse to role datahub_role;
grant select on all external tables in database $your_warehouse to role datahub_role;
grant select on future external tables in database $your_warehouse to role datahub_role;
grant select on all views in database $your_warehouse to role datahub_role;
grant select on future views in database $your_warehouse to role datahub_role;

-- Create a new DataHub user and assign the DataHub role to it 
create user datahub_user display_name = 'DataHub' password='' default_role = datahub_role default_warehouse = $your_warehouse;

-- Grant the datahub_role to the new DataHub user. 
grant role datahub_role to user datahub_user;
