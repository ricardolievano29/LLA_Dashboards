--- ##### LCPR SPRINT 3 - OPERATIONAL DRIVERS - EARLY TICKETS #####

WITH

parameters as (
SELECT date_trunc('month', date('2023-02-01')) as input_month
)

--- --- --- FMC table
, fmc_table as (
SELECT
    *
FROM "db_stage_dev"."lcpr_fmc_table_dec_mar23" --- Make sure to set the month accordindly to the input month of parameters
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_jan_mar23")
UNION ALL (SELECT * FROM "db_stage_dev"."lcpr_fmc_table_feb_mar23")
-- WHERE 
    -- fmc_s_dim_month = (SELECT input_month FROM parameters)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account, --- Operational Drivers are focused just in Fixed dynamics, not Mobile. I don't take the FMC account because sometimes in concatenates Fixed and Mobile accounts ids, which alters the results when joining with other bases using the account id.
    count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null --- Making sure that we are focusing just in Fixed.
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

--- --- --- New customers (2 months ago)

, new_customers2m_pre as (
SELECT
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account 
FROM "lcpr.stage.prod"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(CONNECT_DTE_SBB)) = (SELECT input_month FROM parameters) - interval '2' month
ORDER BY 1
)
    
, new_customers2m as (   
SELECT 
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales2m_flag,
    fix_s_att_account
FROM new_customers2m_pre
)

--- ### Interactions

, clean_interaction_time as (
SELECT *
FROM "lcpr.stage.prod"."lcpr_interactions_csg"
WHERE
    (cast(interaction_start_time as varchar) != ' ') 
    and (interaction_start_time is not null)
    and date_trunc('month', date(interaction_start_time)) between ((SELECT input_month FROM parameters) - interval '2' month) and ((SELECT input_month FROM parameters))
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *,
    date(interaction_start_time) as interaction_date, 
    date_trunc('month', date(interaction_start_time)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(partition by account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip order by interaction_date desc) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

--- ### ### ### Early Tickets

, relevant_interactions as (
SELECT
    customer_id, 
    min(interaction_date) as min_interaction_date, 
    min(date_trunc('month', date(interaction_date))) as interaction_start_month 
FROM interactions_fields2 a
WHERE 
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')
    and (lower(interaction_purpose_descrip) like '%ppv%problem%'
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
    and date_trunc('month',date(interaction_start_time)) between ((SELECT input_month FROM parameters) - interval '2' month) and ((SELECT input_month FROM parameters))
GROUP BY 1
)

, new_customer_interactions AS (
SELECT 
    fix_s_att_account, 
    new_sales2m_flag,
    install_month, 
    interaction_start_month, 
    fix_b_att_maxstart,
    case when date_diff('week', date(fix_b_att_maxstart), date(min_interaction_date)) <= 7 then fix_s_att_account else null end as early_interaction_flag
FROM new_customers2m A 
LEFT JOIN relevant_interactions B 
    ON A.fix_s_att_account = cast(B.customer_id as bigint)
)

, flag_early_tickets as (
SELECT
    F.*, 
    new_sales2m_flag, 
    early_interaction_flag
FROM fmc_table_adj F
LEFT JOIN new_customer_interactions I
    ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
WHERE
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fix_e_att_active = 1 
)

-- SELECT count(*), count(distinct fix_s_att_account) FROM new_customer_interactions --WHERE interaction_start_month is not null
-- SELECT count(fix_s_att_account), count(distinct fix_s_att_account) FROM flag_early_tickets
SELECT count(distinct early_interaction_flag), count(distinct new_sales2m_flag) FROM flag_early_tickets
