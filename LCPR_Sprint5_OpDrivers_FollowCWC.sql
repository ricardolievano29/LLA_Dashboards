--- ##### Sprint 5 - Operational drivers - LCPR #####

WITH

--- ### Get the needed tables

 fixed_table as (
SELECT * FROM "db_stage_dev"."lcpr_fixed_table_jan_feb23"
) --- WARNING: The table name will be updated, so keep the code with the right name.
 
, interactions as (
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

, initial_table as (
SELECT
    interaction_start_time,
    interaction_id,
    date_trunc('Month', date(interaction_start_time)) as ticket_month, 
    account_id, 
    last_value(interaction_start_time) over (partition by account_id, date_trunc('Month', date(interaction_start_time)) order by interaction_start_time) as last_int_dt
FROM interactions
)

, tickets_count as (
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

, repair_times as (
SELECT
    date_trunc('Month', date(interaction_start_time)) as Repair_Month, 
    account_id, 
    interaction_start_time, 
    interaction_end_time, 
    date_diff('day', interaction_start_time, interaction_end_time) as solving_time
FROM interactions
WHERE interaction_status = 'Closed'
)
-- solving_time is basically a column filled with 0s, is this right?

, outlier_times as (
SELECT
    Repair_Month, 
    account_id, 
    interaction_start_time, 
    interaction_end_time, 
    solving_time, 
    case when max(solving_time) > 4 then account_id else null end as outlier_repair
FROM repair_times
GROUP BY Repair_Month, account_id, interaction_start_time, interaction_end_time, solving_time
ORDER BY Repair_Month desc, solving_time desc, account_id
)

, outlier_repair_flag as (
SELECT
    f.*, 
    case when length(cast(f.fix_s_att_account as varchar)) = 8 then 'Cerillion' else 'Liberate' end as CRM, 
    Repair_Month, 
    case when account_id is not null then account_id else null end as techticket, 
    outlier_repair
FROM reiteractiontickets_flags f
LEFT JOIN outlier_times o 
    ON cast(f.fix_s_att_account as varchar) = cast(o.account_id as varchar) and f.fix_s_dim_month = o.Repair_Month
)

--- It works (24/2/2023 11:39am)

--- #### Tech tickets density

, tickets_per_account as (
SELECT 
    date_trunc('Month', date(interaction_start_time)) as Ticket_Month, 
    account_id, 
    count(distinct interaction_id) as num_tickets
FROM interactions
GROUP BY date_trunc('Month', date(interaction_start_time)), account_id
)

, records_fixed_accounts as (
SELECT 
    distinct fix_s_dim_month, 
    fix_s_att_account, 
    count(*) as num_records --- What about the use of distinct?
FROM outlier_repair_flag
-- WHERE fix_s_dim_month = date(fix_e_att_maxstart) --- Is this the right month choose? There is no column called simply 'Month' as in Official CJW Sprint 5 code in GitLab nor dt.
GROUP BY distinct fix_s_dim_month, fix_s_att_account
)

, ticket_density_flag as (
SELECT 
    f.*, 
    num_tickets, 
    num_records, 
    (num_tickets/num_records) as adj_tickets
FROM outlier_repair_flag f
INNER JOIN records_fixed_accounts r
    ON cast(f.fix_s_att_account as varchar) = cast(r.fix_s_att_account as varchar) and f.fix_s_dim_month = r.fix_s_dim_month
LEFT JOIN tickets_per_account t
    ON cast(f.fix_s_att_account as varchar) = cast(t.account_id as varchar) and f.fix_s_dim_month = t.Ticket_Month
)

, results_table_S5 as (
SELECT
    fix_s_dim_month, 
    fix_e_fla_tech,
    fix_e_fla_tech, 
    count(distinct fix_s_att_account) as activebase, 
    count(distinct one_tckt) as one_ticket, 
    count(distinct over1_tckt) as over1_ticket, 
    count(distinct two_tckt) as two_tickets, 
    count(distinct three_tckt) as three_more_tickets, 
    count(distinct techticket) as ticket_customers, 
    sum(adj_tickets) as total_tickets, 
    count(distinct outlier_repair) as outlier_repairs
FROM ticket_density_flag
WHERE
    fix_s_fla_churnflag != '1. Fixed Churner'
    --- and waterfall_flag != 'Downsell-Fixed Customer Gap'
    --- and waterfall_flag != 'Fixed Base Exceptions'
    and fix_s_fla_mainmovement != '6.Null last day'
    --- and waterfall_flag != 'Churn Exception'
    --- and month = date(dt)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
)

SELECT distinct * FROM results_table_S5

--- It seems that everything works! (24/2/2023 3:30pm)
--- It only shows info for January 2023.

-- SELECT count(distinct fix_s_att_account) FROM fixed_table WHERE fix_e_fla_tech = 'HFC'

--- Interaction start date and interaction end date are always the same in interactions PR, so we can't flag outlier repairs.
