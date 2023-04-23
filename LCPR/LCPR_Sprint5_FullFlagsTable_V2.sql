--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - FULL FLAGS TABLE ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH

parameters as (SELECT date('2023-03-01') as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_prod"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
GROUP BY 1, 2
ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Interactions and truckrolls --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    cast(interaction_start_time as varchar) != ' ' 
    and interaction_start_time is not null
    and date_trunc('month', date(interaction_start_time)) between ((SELECT input_month FROM parameters) - interval '2' month) and (SELECT input_month FROM parameters) 
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *, 
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(PARTITION BY account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip ORDER BY interaction_date DESC) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

, truckrolls as (
SELECT 
    sub_acct_no_sbb,
    job_no_ojb,
    job_typ_ojb,
    job_stat_ojb,
    create_dte_ojb, 
    comp_dte, 
    resched_dte_ojb
FROM "lcpr.stage.dev"."truckrolls" 
WHERE 
    date_trunc('month', date(create_dte_ojb)) between ((SELECT input_month FROM parameters) - interval '2' month) and ((SELECT input_month FROM parameters))
)

, full_interactions as (
SELECT 
    *, 
    
    case
        when interaction_start_time is not null then date_trunc('month', date(interaction_start_time))
        when (interaction_start_time is null and create_dte_ojb is not null) then date_trunc('month', date(create_dte_ojb))
    else null end as ticket_month,
    
    case 
    
        when create_dte_ojb is not null then 'truckroll'
    
        when (
        lower(interaction_purpose_descrip) like '%ppv%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%problem%'
        or lower(interaction_purpose_descrip) like '%cable%problem%'
        or lower(interaction_purpose_descrip) like '%tv%problem%'
        or lower(interaction_purpose_descrip) like '%video%problem%'
        or lower(interaction_purpose_descrip) like '%tel%problem%'
        or lower(interaction_purpose_descrip) like '%phone%problem%'
        or lower(interaction_purpose_descrip) like '%int%problem%'
        or lower(interaction_purpose_descrip) like '%line%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%issue%'
        or lower(interaction_purpose_descrip) like '%ppv%issue%'
        or lower(interaction_purpose_descrip) like '%video%issue%'
        or lower(interaction_purpose_descrip) like '%tel%issue%'
        or lower(interaction_purpose_descrip) like '%phone%issue%'
        or lower(interaction_purpose_descrip) like '%int%issue%'
        or lower(interaction_purpose_descrip) like '%line%issue%'
        or lower(interaction_purpose_descrip) like '%cable%issue%'
        or lower(interaction_purpose_descrip) like '%tv%issue%'
        or lower(interaction_purpose_descrip) like '%bloq%'
        or lower(interaction_purpose_descrip) like '%slow%'
        or lower(interaction_purpose_descrip) like '%slow%service%'
        or lower(interaction_purpose_descrip) like '%service%tech%'
        or lower(interaction_purpose_descrip) like '%tech%service%'
        or lower(interaction_purpose_descrip) like '%no%service%'
        or lower(interaction_purpose_descrip) like '%hsd%no%'
        or lower(interaction_purpose_descrip) like '%hsd%slow%'
        or lower(interaction_purpose_descrip) like '%hsd%intermit%'
        or lower(interaction_purpose_descrip) like '%no%brows%'
        or lower(interaction_purpose_descrip) like '%phone%cant%'
        or lower(interaction_purpose_descrip) like '%phone%no%'
        or lower(interaction_purpose_descrip) like '%no%connect%'
        or lower(interaction_purpose_descrip) like '%no%conect%'
        or lower(interaction_purpose_descrip) like '%no%start%'
        or lower(interaction_purpose_descrip) like '%equip%'
        or lower(interaction_purpose_descrip) like '%intermit%'
        or lower(interaction_purpose_descrip) like '%no%dat%'
        or lower(interaction_purpose_descrip) like '%dat%serv%'
        or lower(interaction_purpose_descrip) like '%int%data%'
        or lower(interaction_purpose_descrip) like '%tech%'
        or lower(interaction_purpose_descrip) like '%supp%'
        or lower(interaction_purpose_descrip) like '%outage%'
        or lower(interaction_purpose_descrip) like '%mass%'
        or lower(interaction_purpose_descrip) like '%discon%warn%'
        ) and (
        lower(interaction_purpose_descrip) not like '%work%order%status%'
        and lower(interaction_purpose_descrip) not like '%default%call%wrapup%'
        and lower(interaction_purpose_descrip) not like '%bound%call%'
        and lower(interaction_purpose_descrip) not like '%cust%first%'
        and lower(interaction_purpose_descrip) not like '%audit%'
        and lower(interaction_purpose_descrip) not like '%eq%code%'
        and lower(interaction_purpose_descrip) not like '%downg%'
        and lower(interaction_purpose_descrip) not like '%upg%'
        and lower(interaction_purpose_descrip) not like '%vol%discon%'
        and lower(interaction_purpose_descrip) not like '%discon%serv%'
        and lower(interaction_purpose_descrip) not like '%serv%call%'
        )
        then 'tech_call'
        
        else null
        
        end as interaction_type
        
FROM interactions_fields2 a
FULL OUTER JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Repeated callers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, last_interaction as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over (partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM interactions_fields
)

, join_last_interaction as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    date_trunc('month', last_interaction_date) as last_interaction_month, 
    last_interaction_date, 
    date_add('DAY', -60, last_interaction_date) as window_day
FROM interactions_fields W
INNER JOIN last_interaction L
    ON W.account_id = L.last_account
)

, interactions_count as (
SELECT
    distinct account_id,
    last_interaction_month, 
    count(distinct interaction_id) as interactions
FROM join_last_interaction
WHERE
    interaction_date between window_day and last_interaction_date
GROUP BY 1, 2
)

, interactions_tier as (
SELECT
    *, 
    case 
        when interactions = 1 then '1'
        when interactions = 2 then '2'
        when interactions >= 3 then '>3'
        else null
    end as interaction_tier
FROM interactions_count
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Reiterative tickets --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, users_tickets as (
SELECT 
    *, 
    case when interaction_type in ('truckroll','tech_call') then interaction_id else null end as number_tickets
FROM full_interactions
)

, last_ticket as (
SELECT 
    account_id as last_account, 
    first_value(interaction_date) over(partition by account_id, date_trunc('month', interaction_date) order by interaction_date desc) as last_interaction_date
FROM users_tickets
)

, join_last_ticket as (
SELECT
    account_id, 
    interaction_id, 
    interaction_date, 
    date_trunc('month', last_interaction_date) as last_interaction_month, 
    last_interaction_date, 
    date_add('day', -60, last_interaction_date) as window_day, 
    number_tickets
FROM users_tickets W
INNER JOIN last_ticket L
    ON W.account_id = L.last_account
)

, tickets_count as (
SELECT 
    last_interaction_month, 
    account_id, 
    interaction_date,
    last_interaction_date,
    count(distinct number_tickets) as tickets
FROM join_last_ticket
WHERE interaction_date between window_day and last_interaction_date
GROUP BY 1, 2, 3, 4
)

, tickets_tier as (
SELECT 
    *,
    case
        when tickets = 1 then '1'
        when tickets = 2 then '2'
        when tickets >= 3 then '>3'
    else null end as ticket_tier
FROM tickets_count
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Outlier Repairs --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, relevant_truckrolls_outlier_repairs as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    comp_dte, 
    case when date_diff('day', date(create_dte_ojb), date(comp_dte)) > 4 then sub_acct_no_sbb else null end as outlier_repair
FROM truckrolls
WHERE 
    job_typ_ojb != 'MN'
    and date_trunc('month', date(create_dte_ojb)) = (SELECT input_month FROM parameters)
    and comp_dte like '%-%-%'
    and job_stat_ojb = 'C'
)

, outlier_repairs as (
SELECT
    distinct sub_acct_no_sbb as account_id, 
    job_month,
    max(outlier_repair) as outlier_repair_flag
FROM relevant_truckrolls_outlier_repairs
GROUP BY 1, 2
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Missed Visits --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, relevant_truckrolls_missed_visits as (
SELECT
    date_trunc('month', date(create_dte_ojb)) as job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    comp_dte, 
    resched_dte_ojb,
    job_stat_ojb
FROM truckrolls
WHERE 
    job_typ_ojb != 'MN'
    and date_trunc('month', date(create_dte_ojb)) between ((SELECT input_month FROM parameters) - interval '2' month) and (SELECT input_month FROM parameters)
    and comp_dte like '%-%-%'
    and job_stat_ojb in ('C', 'R')
    -- and resched_dte_ojb not like '00%-%-%'
)

, last_truckroll as (
SELECT
    sub_acct_no_sbb as last_account, 
    first_value(create_dte_ojb) over (partition by sub_acct_no_sbb, date_trunc('month', date(create_dte_ojb)) order by create_dte_ojb desc) as last_job_date
FROM relevant_truckrolls_missed_visits
)

, join_last_truckroll as (
SELECT
    -- date_trunc('month', date(create_dte_ojb)) as job_month,
    date_trunc('month', date(last_job_date)) as last_job_month,
    sub_acct_no_sbb,
    job_no_ojb,
    create_dte_ojb, 
    last_job_date, 
    case when (job_stat_ojb = 'R' or resched_dte_ojb not like '00%-%-%') then sub_acct_no_sbb else null end as missed_visit,
    date_add('DAY', -60, date(last_job_date)) as window_day
FROM relevant_truckrolls_missed_visits W
INNER JOIN last_truckroll L
    ON W.sub_acct_no_sbb = L.last_account
)

, missed_visits as (
SELECT
    distinct sub_acct_no_sbb as account_id, 
    -- job_month,
    create_dte_ojb,
    last_job_date,
    last_job_month,
    max(missed_visit) as missed_visit_flag
FROM join_last_truckroll
WHERE
    date(create_dte_ojb) between window_day and date(last_job_date)
GROUP BY 1, 2, 3, 4
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tickets per month --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, users_tickets_per_month as (
SELECT
    distinct account_id, 
    ticket_month,
    interaction_id, 
    interaction_date, 
    interaction_type
FROM full_interactions
WHERE 
    date(ticket_month) = (SELECT input_month FROM parameters)
)

, tickets_per_month as (
SELECT
    ticket_month, 
    account_id,
    case when interaction_type in ('tech_call', 'truckroll') then interaction_id else null end as tickets_flag
FROM users_tickets_per_month
WHERE interaction_id is not null
-- GROUP BY 1, 2
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, interaction_tier_flag as(
SELECT 
    F.*,
    interactions,
    interaction_tier
FROM fmc_table_adj F
LEFT JOIN interactions_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) 
       and F.fmc_s_dim_month = I.last_interaction_month
WHERE date(fmc_s_dim_month) = (SELECT input_month FROM parameters)
)

, ticket_tier_flag as (
SELECT 
    F.*, 
    ticket_tier
FROM interaction_tier_flag F
LEFT JOIN tickets_tier I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.last_interaction_month
)

, outlier_repairs_flag as (
SELECT 
    F.*, 
    outlier_repair_flag
FROM ticket_tier_flag F
LEFT JOIN outlier_repairs I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.job_month
)

, missed_visits_flag as (
SELECT 
    F.*, 
    missed_visit_flag
FROM outlier_repairs_flag F
LEFT JOIN missed_visits I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) 
        and (F.fmc_s_dim_month = I.last_job_month)
)

, number_tickets_flag as (
SELECT
    F.*, 
    tickets_flag
FROM missed_visits_flag F 
LEFT JOIN tickets_per_month I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.ticket_month
)

, final_table as (
SELECT 
    fmc_s_dim_month as odr_s_dim_month,
    fmc_e_fla_tech as odr_e_fla_final_tech,
    fmc_e_fla_fmcsegment as odr_e_fla_fmc_segment,
    fmc_e_fla_fmc as odr_e_fla_fmc_type,
    case 
        when fmc_e_fla_tenure = 'Early Tenure' then 'Early-Tenure'
        when fmc_e_fla_tenure = 'Mid Tenure' then 'Mid-Tenure'
        when fmc_e_fla_tenure = 'Late Tenure' then 'Late-Tenure'
    end as odr_e_fla_final_tenure,
    interaction_tier as odr_s_fla_interaction_tier, 
    -- ticket_tier as odr_s_fla_tickets_tier, --- Omitted because causes duplication of tickets per month
    count(distinct case when fix_e_att_active = 1 then fix_s_att_account else null end) as odr_s_mes_active_base,
    count(distinct fix_s_att_account) as odr_s_mes_total_accounts, 
    count(distinct case when ticket_tier = '1' then fix_s_att_account else null end) as odr_s_mes_one_ticket,  
    count(distinct case when ticket_tier in ('2', '>3') then fix_s_att_account else null end) as odr_s_mes_over1_ticket,
    count(distinct case when ticket_tier = '2' then fix_s_att_account else null end) as odr_s_mes_two_tickets, 
    count(distinct interactions) as odr_s_mes_user_interactions,
    count(distinct tickets_flag) as odr_s_mes_number_tickets,
    count(distinct outlier_repair_flag) as odr_s_mes_outlier_repairs, 
    count(distinct missed_visit_flag) as odr_s_mes_missed_visits
FROM number_tickets_flag
WHERE
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', '6.Null last day', 'Churn Exception')
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM final_table

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

--- --- --- ### ### ### Repeated callers (60-day moving window)

-- SELECT
--     odr_s_fla_interaction_tier,
--     sum(odr_s_mes_total_accounts) as num_cliets
-- FROM final_table
-- GROUP BY 1

--- --- --- ### ### ### Reiterative tickets (60-day moving window)

-- SELECT
--     sum(odr_s_mes_one_ticket) as clients_w_one_ticket,
--     sum(odr_s_mes_two_tickets) as clients_w_two_tickets,
--     sum(odr_s_mes_over1_ticket) as clients_w_over_one_tickets
-- FROM final_table
-- GROUP BY 1

--- --- --- ### ### ### Outlier repairs (4 days)

-- SELECT
--     sum(odr_s_mes_outlier_repairs) as Outlier_Repairs,
--     sum(odr_s_mes_active_base) as Active_Base, 
--     cast(sum(odr_s_mes_outlier_repairs) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### Missed visits

-- SELECT
--     sum(odr_s_mes_missed_visits) as missed_visits,
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(odr_s_mes_missed_visits) as double)/cast(sum(odr_s_mes_active_base) as double) as KPI
-- FROM final_table

--- --- --- ### ### ### Tickets per 100 users (Tickets per month)

-- SELECT
--     sum(odr_s_mes_number_tickets) as number_tickets,
--     sum(odr_s_mes_active_base) as active_base, 
--     cast(sum(odr_s_mes_number_tickets) as double)/(cast(sum(odr_s_mes_active_base) as double)/100) as tickets_per_100_users
-- FROM final_table

--- --- --- ### ### ### Nodes ticket density (Is in another query with the CX table structure)


