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

,initial_table as (
SELECT
    interaction_start_time,
    interaction_id,
    date_trunc('Month', date(interaction_start_time)) as ticket_month, 
    account_id, 
    last_value(interaction_start_time) over (partition by account_id, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt
FROM interactions
)

,tickets_count as (
SELECT 
    Ticket_Month, 
    account_id, 
    count(distinct interaction_id) as tickets
FROM initial_table
WHERE interaction_start_time between (last_int_dt - interval '60' day) and last_int_dt
GROUP BY Ticket_Month, account_id
)

, reiteractions_summary as (
SELECT
    *,
    case when tickets = 1 then account_id else null end as one_tckt, 
    case when tickets > 1 then account_id else null end as over1_tckt, 
    case when tickets = 2 then account_id else null end as two_tckt, 
    case when tickets >= 3 then account_id else null end as three_tckt
FROM tickets_count
)

, reiteractiontickets_flags as (
SELECT 
    f.*,
    Ticket_Month as RTicket_Month, 
    one_tckt, 
    over1_tckt, 
    two_tckt, 
    three_tckt
FROM fixed_table f
LEFT JOIN reiteractions_summary r
    ON cast(f.fix_s_att_account as varchar) = cast(account_id as varchar) and f.fix_s_dim_month = r.Ticket_Month
)

--- It works (23/02/2023 2:00pm)

--- #### Outlier repair times - using interactions

repair_times as (
SELECT
    date_trun('Month', date(interaction_start_time)) as Repair_Month, 
    account_id as account, 
    interaction_start_time, 
    interaction_end_time, 
    date_diff('day', interaction_start_time, interaction_end_time) as solving_time
FROM interactions
WHERE   interaction_status = 'CLOSED'
)
