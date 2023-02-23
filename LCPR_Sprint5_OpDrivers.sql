--- ##### Sprint 5 - Operational drivers - LCPR #####

WITH

--- ### Get the needed tables

fixed_table as (
SELECT * FROM "db_stage_dev"."lcpr_fixed_table_jan_feb23"
) --- WARNING: The table name will be updated, so keep the code with the right name.
 
,interactions as (
SELECT * FROM "lcpr.stage.prod"."lcpr_interactions_csg"
)
    
-- ,fmc_table as 
--     (SELECT 
--         *
--     FROM "db_stage_dev"."lcpr_fixed_table_jan_feb20" f
--     INNER JOIN "db_stage_dev"."lcpr_convergency_jan_feb23" c
--         ON f.fix_s_att_account = c.fixed_account
--     INNER JOIN "db_stage_dev"."lcpr_mobile_table_jan_feb23" m
--         ON m.mob_s_att_account = c.mobile_account)
     
--- #### Reiterative tickets

-- ,initial_table as (
SELECT
    date_trunc('Month', date(interaction_start_time)) as ticket_month, 
    account_id, 
    last_value(interaction_start_time) over (partition by account_id, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt
FROM interactions
-- )
