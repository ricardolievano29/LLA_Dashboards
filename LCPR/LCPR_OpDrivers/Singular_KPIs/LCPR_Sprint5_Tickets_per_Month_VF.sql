--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 5 - OPERATIONAL DRIVERS - TICKETS PER MONTH ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
WITH

 parameters as (
 SELECT date_trunc('month', date('2023-03-01')) as input_month --- Input month you wish the code run for
 )

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Interactions and truckrolls --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    cast(interaction_start_time as varchar) != ' ' 
    and interaction_start_time is not null
    and date_trunc('month', date(interaction_start_time)) = (SELECT input_month FROM parameters)
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
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls" 
)

, full_interactions as (
SELECT 
    *, 
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
--- --- --- --- --- --- --- --- --- --- --- Tickets per month --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, users_tickets as (
SELECT
    distinct account_id, 
    interaction_id, 
    interaction_date, 
    interaction_type
FROM full_interactions
)

, tickets_per_month as (
SELECT
    date_trunc('month', interaction_date) as month, 
    account_id,
    count(distinct case when interaction_type in ('tech_call', 'truckroll') then interaction_id end) as number_tickets
FROM users_tickets
WHERE interaction_id is not null
GROUP BY 1, 2
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final Flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, number_tickets_flag as (
SELECT
    F.*, 
    number_tickets
FROM fmc_table_adj F 
LEFT JOIN tickets_per_month I
    ON cast(F.fix_s_att_account as varchar) = cast(I.account_id as varchar) and F.fmc_s_dim_month = I.month
WHERE 
    fix_e_att_active = 1
)

, final_table as (
SELECT
    fmc_s_dim_month as opd_s_dim_month,
    fix_b_fla_tech as fmc_b_fla_tech_type,
    fix_b_fla_fmc as fmc_b_fla_fmc_status,
    fix_b_fla_mixcodeadj as fmc_b_dim_mix_code_adj,
    fix_e_fla_tech as fmc_e_fla_tech_type,
    fix_e_fla_fmc as fmc_e_fla_fmc_status,
    fix_e_fla_mixcodeadj as fmc_e_dim_mix_code_adj,
    fix_b_fla_tenure as fmc_b_fla_final_tenure,
    fix_e_fla_tenure as fmc_e_fla_final_tenure,
    fix_s_fla_churntype as fmc_s_fla_churn_type,
    fix_s_fla_mainmovement as fmc_s_fla_main_movement,
    count(distinct fix_s_att_account) as odr_s_mes_total_accounts,
    sum(number_tickets) as odr_s_mes_number_tickets
FROM number_tickets_flag
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

SELECT * FROM final_table

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Tests --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

-- SELECT
--     sum(odr_s_mes_number_tickets) as number_tickets,
--     sum(odr_s_mes_total_accounts) as active_base, 
--     cast(sum(odr_s_mes_number_tickets) as double)/(cast(sum(odr_s_mes_total_accounts) as double)/100) as tickets_per_100_users
-- FROM final_table
